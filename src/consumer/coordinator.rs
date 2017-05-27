use std::rc::Rc;
use std::cell::RefCell;
use std::borrow::Cow;
use std::iter::FromIterator;
use std::collections::{HashMap, HashSet};

use futures::Future;

use errors::{ErrorKind, Result};
use protocol::{KafkaCode, Schema};
use client::{BrokerRef, Client, ConsumerGroupAssignment, ConsumerGroupMember,
             ConsumerGroupProtocol, Generation, KafkaClient, Metadata, StaticBoxFuture};
use consumer::{CONSUMER_PROTOCOL, PartitionAssignor, Subscription, Subscriptions};

/// Manages the coordination process with the consumer coordinator.
pub trait Coordinator {
    /// Join the consumer group.
    fn join_group(&mut self) -> JoinGroup;

    /// Leave the current consumer group.
    fn leave_group(&mut self) -> LeaveGroup;
}

pub type JoinGroup = StaticBoxFuture;

pub type LeaveGroup = StaticBoxFuture;

/// Manages the coordination process with the consumer coordinator.
pub struct ConsumerCoordinator<'a> {
    inner: Rc<Inner<'a>>,
}

struct Inner<'a> {
    client: KafkaClient<'a>,
    group_id: String,
    subscriptions: RefCell<Subscriptions>,
    session_timeout: i32,
    rebalance_timeout: i32,
    assignors: Vec<Box<PartitionAssignor>>,
    state: Rc<RefCell<State>>,
}

enum State {
    /// the client is not part of a group
    Unjoined,
    /// the client has begun rebalancing
    Rebalancing,
    /// the client has joined and is sending heartbeats
    Stable {
        coordinator: BrokerRef,
        generation: Generation,
    },
}

impl State {
    pub fn member_id(&self) -> Option<String> {
        if let State::Stable { ref generation, .. } = *self {
            Some(generation.member_id.clone())
        } else {
            None
        }
    }
}

impl<'a> ConsumerCoordinator<'a> {
    pub fn new(client: KafkaClient<'a>,
               group_id: String,
               subscriptions: Subscriptions,
               session_timeout: i32,
               rebalance_timeout: i32,
               assignors: Vec<Box<PartitionAssignor>>)
               -> Self {
        ConsumerCoordinator {
            inner: Rc::new(Inner {
                               client: client,
                               group_id: group_id,
                               subscriptions: RefCell::new(subscriptions),
                               session_timeout: session_timeout,
                               rebalance_timeout: rebalance_timeout,
                               assignors: assignors,
                               state: Rc::new(RefCell::new(State::Unjoined)),
                           }),
        }
    }
}

impl<'a> Inner<'a>
    where Self: 'static
{
    fn group_protocols(&self) -> Vec<ConsumerGroupProtocol<'a>> {
        let topics: Vec<String> = self.subscriptions
            .borrow()
            .topics()
            .iter()
            .map(|topic_name| String::from(*topic_name))
            .collect();

        self.assignors
            .iter()
            .flat_map(move |assignor| {
                let subscription =
                    assignor.subscription(topics
                                              .iter()
                                              .map(|topic_name| topic_name.as_str().into())
                                              .collect());

                Schema::serialize(&subscription)
                    .map_err(|err| warn!("fail to serialize subscription, {}", err))
                    .ok()
                    .map(|metadata| {
                             ConsumerGroupProtocol {
                                 protocol_name: assignor.name().into(),
                                 protocol_metadata: metadata.into(),
                             }
                         })
            })
            .collect()
    }

    fn perform_assignment(&self,
                          metadata: &Metadata,
                          group_protocol: &str,
                          members: &[ConsumerGroupMember])
                          -> Result<Vec<ConsumerGroupAssignment<'a>>> {
        let strategy = group_protocol.parse()?;
        let assignor = self.assignors
            .iter()
            .find(|assigner| assigner.strategy() == strategy)
            .ok_or_else(|| ErrorKind::UnsupportedAssignmentStrategy(group_protocol.to_owned()))?;

        let mut subscripbed_topics = HashSet::new();
        let mut subscriptions = HashMap::new();

        for member in members {
            let subscription: Subscription = Schema::deserialize(member.member_metadata.as_ref())?;

            subscripbed_topics.extend(subscription.topics.iter().cloned());
            subscriptions.insert(member.member_id.as_str().into(), subscription);
        }

        let assignment = assignor.assign(metadata, subscriptions);

        // user-customized assignor may have created some topics that are not in the subscription
        // list and assign their partitions to the members; in this case we would like to update the
        // leader's own metadata with the newly added topics so that it will not trigger a
        // subsequent rebalance when these topics gets updated from metadata refresh.

        let mut assigned_topics = HashSet::new();

        assigned_topics.extend(assignment
                                   .values()
                                   .flat_map(|member| {
                                                 member.partitions.iter().map(|tp| {
                                                                                  tp.topic_name
                                                                                      .clone()
                                                                              })
                                             }));

        let not_assigned_topics = &subscripbed_topics - &assigned_topics;

        if !not_assigned_topics.is_empty() {
            warn!("The following subscribed topics are not assigned to any members in the group `{}`: {}",
                  self.group_id,
                  Vec::from_iter(not_assigned_topics.iter().cloned())
                      .as_slice()
                      .join(","));
        }

        let newly_added_topics = &assigned_topics - &subscripbed_topics;

        if !newly_added_topics.is_empty() {
            info!("The following not-subscribed topics are assigned to group {}, and their metadata will be fetched from the brokers : {}",
                  self.group_id,
                  Vec::from_iter(newly_added_topics.iter().cloned())
                      .as_slice()
                      .join(","));

            subscripbed_topics.extend(assigned_topics);
        }

        self.subscriptions
            .borrow_mut()
            .group_subscribe(subscripbed_topics.iter());

        let mut group_assignment = Vec::new();

        for (member_id, assignment) in assignment {
            group_assignment.push(ConsumerGroupAssignment {
                                      member_id: String::from(member_id).into(),
                                      member_assignment: Schema::serialize(&assignment)?.into(),
                                  })
        }

        Ok(group_assignment)
    }
}

impl<'a> Coordinator for ConsumerCoordinator<'a>
    where Self: 'static
{
    fn join_group(&mut self) -> JoinGroup {
        let member_id = self.inner.state.borrow().member_id().unwrap_or_default();

        (*self.inner.state.borrow_mut()) = State::Rebalancing;

        let inner = self.inner.clone();
        let client = self.inner.client.clone();
        let group_id: Cow<'a, str> = self.inner.group_id.clone().into();
        let session_timeout = self.inner.session_timeout;
        let rebalance_timeout = self.inner.rebalance_timeout;
        let group_protocols = self.inner.group_protocols();
        let state = self.inner.state.clone();

        let future = self.inner
            .client
            .metadata()
            .join(self.inner.client.group_coordinator(group_id.clone()))
            .and_then(move |(metadata, coordinator)| {
                client
                    .join_group(coordinator.as_ref(),
                                group_id,
                                session_timeout,
                                rebalance_timeout,
                                member_id.into(),
                                CONSUMER_PROTOCOL.into(),
                                group_protocols)
                    .and_then(move |consumer_group| {
                        let generation = consumer_group.generation();

                        let group_assignment = if consumer_group.is_leader() {
                            match inner.perform_assignment(&metadata,
                                                           &consumer_group.protocol,
                                                           &consumer_group.members) {
                                Ok(group_assignment) => group_assignment,
                                Err(err) => return JoinGroup::err(err),
                            }
                        } else {
                            Vec::new()
                        };

                        let future = client
                            .sync_group(coordinator.as_ref(),
                                        consumer_group.group_id.clone().into(),
                                        generation.generation_id.into(),
                                        generation.member_id.clone().into(),
                                        group_assignment)
                            .then(move |result| {
                                *state.borrow_mut() = match result {
                                    Ok(_) => {
                                        State::Stable {
                                            coordinator: coordinator.as_ref(),
                                            generation: generation,
                                        }
                                    }
                                    Err(_) => State::Unjoined,
                                };

                                result.map(|_| ())
                            });

                        JoinGroup::new(future)
                    })
            });

        JoinGroup::new(future)
    }

    fn leave_group(&mut self) -> LeaveGroup {
        if let State::Stable {
                   coordinator,
                   ref generation,
               } = *self.inner.state.borrow() {
            debug!("sending LeaveGroup request to coordinator {:?} for group `{}`",
                   coordinator,
                   self.inner.group_id);

            (*self.inner.state.borrow_mut()) = State::Rebalancing;

            let group_id = self.inner.group_id.clone().into();
            let state = self.inner.state.clone();

            LeaveGroup::new(self.inner
                                .client
                                .leave_group(coordinator,
                                             group_id,
                                             generation.member_id.clone().into())
                                .map(move |group_id| {
                                         debug!("leaved group {}", group_id);

                                         *state.borrow_mut() = State::Unjoined;
                                     }))
        } else {
            LeaveGroup::err(ErrorKind::KafkaError(KafkaCode::GroupLoadInProgress).into())
        }
    }
}

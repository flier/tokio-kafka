use std::rc::Rc;
use std::cell::RefCell;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use futures::Future;

use errors::{ErrorKind, Result};
use protocol::{KafkaCode, Schema};
use client::{BrokerRef, Client, ConsumerGroupAssignment, ConsumerGroupMember,
             ConsumerGroupProtocol, Generation, KafkaClient, StaticBoxFuture};
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
    subscriptions: Subscriptions,
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
        if let &State::Stable { ref generation, .. } = self {
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
                               subscriptions: subscriptions,
                               session_timeout: session_timeout,
                               rebalance_timeout: rebalance_timeout,
                               assignors: assignors,
                               state: Rc::new(RefCell::new(State::Unjoined)),
                           }),
        }
    }
}

impl<'a> Inner<'a> {
    fn group_protocols(&self) -> Vec<ConsumerGroupProtocol<'a>> {
        let topics = self.subscriptions.topics();

        self.assignors
            .iter()
            .flat_map(move |assignor| {
                let subscription = assignor.subscription(topics
                                                             .iter()
                                                             .map(|topic_name| {
                                                                      String::from(*topic_name)
                                                                          .into()
                                                                  })
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
            subscriptions.insert(member.member_id.clone(), subscription);
        }

        Ok(vec![])
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
            .group_coordinator(group_id.clone())
            .and_then(move |coordinator| {
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
                            match inner.perform_assignment(&consumer_group.protocol,
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

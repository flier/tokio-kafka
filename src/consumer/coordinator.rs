use std::mem;
use std::rc::Rc;
use std::cell::RefCell;
use std::time::{Duration, Instant};
use std::iter::FromIterator;
use std::collections::{HashMap, HashSet};

use futures::{Future, Stream, future};
use futures::future::Either;
use tokio_timer::Timer;
use tokio_retry::Retry;

use errors::{Error, ErrorKind, Result, ResultExt};
use protocol::{KafkaCode, Schema, ToMilliseconds};
use client::{BrokerRef, Client, ConsumerGroupAssignment, ConsumerGroupMember,
             ConsumerGroupProtocol, Generation, KafkaClient, Metadata, StaticBoxFuture};
use consumer::{Assignment, CONSUMER_PROTOCOL, PartitionAssignor, Subscription, Subscriptions};

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
    subscriptions: Rc<RefCell<Subscriptions<'a>>>,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    heartbeat_interval: Duration,
    assignors: Vec<Box<PartitionAssignor>>,
    state: Rc<RefCell<State>>,
    timer: Rc<Timer>,
}

#[derive(Clone, Debug, PartialEq)]
enum State {
    /// the client is not part of a group
    Unjoined,
    /// the client has begun rebalancing
    Rebalancing { coordinator: BrokerRef },
    /// the client has joined and is sending heartbeats
    Stable {
        coordinator: BrokerRef,
        generation: Generation,
    },
}

impl State {
    pub fn member_id(&self) -> Option<String> {
        if let State::Stable { ref generation, .. } = *self {
            Some(String::from(generation.member_id.to_owned()))
        } else {
            None
        }
    }

    pub fn rebalance(&mut self, coordinator: BrokerRef) -> Self {
        mem::replace(self, State::Rebalancing { coordinator: coordinator })
    }

    pub fn joined(&mut self, coordinator: BrokerRef, generation: Generation) -> State {
        mem::replace(self,
                     State::Stable {
                         coordinator: coordinator,
                         generation: generation,
                     })
    }

    pub fn leave(&mut self) -> Self {
        mem::replace(self, State::Unjoined)
    }
}

impl<'a> ConsumerCoordinator<'a> {
    pub fn new(client: KafkaClient<'a>,
               group_id: String,
               subscriptions: Rc<RefCell<Subscriptions<'a>>>,
               session_timeout: Duration,
               rebalance_timeout: Duration,
               heartbeat_interval: Duration,
               assignors: Vec<Box<PartitionAssignor>>,
               timer: Rc<Timer>)
               -> Self {
        ConsumerCoordinator {
            inner: Rc::new(Inner {
                               client: client,
                               group_id: group_id,
                               subscriptions: subscriptions,
                               session_timeout: session_timeout,
                               rebalance_timeout: rebalance_timeout,
                               heartbeat_interval: heartbeat_interval,
                               assignors: assignors,
                               timer: timer,
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
                    .map_err(|err| warn!("fail to serialize subscription schema, {}", err))
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
        let strategy =
            group_protocol
                .parse()
                .chain_err(|| format!("fail to parse group protocol: {}", group_protocol))?;
        let assignor = self.assignors
            .iter()
            .find(|assigner| assigner.strategy() == strategy)
            .ok_or_else(|| ErrorKind::UnsupportedAssignmentStrategy(group_protocol.to_owned()))?;

        let mut subscripbed_topics = HashSet::new();
        let mut subscriptions = HashMap::new();

        for member in members {
            let subscription: Subscription =
                Schema::deserialize(member.member_metadata.as_ref())
                    .chain_err(|| "fail to deserialize member metadata schema")?;

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
                                      member_assignment: Schema::serialize(&assignment)
                                          .chain_err(|| "fail to serialize assignment schema")?
                                          .into(),
                                  })
        }

        Ok(group_assignment)
    }

    fn synced_group(&self,
                    assignment: Assignment<'a>,
                    coordinator: BrokerRef,
                    generation: Generation)
                    -> Result<()> {
        trace!("member `{}` synced up to generation # {} with {} partitions: {:?}",
               generation.member_id,
               generation.generation_id,
               assignment.partitions.len(),
               assignment.partitions);

        self.subscriptions
            .borrow_mut()
            .assign_from_subscribed(assignment.partitions)
            .chain_err(|| "fail to assign subscribed partitions")?;

        self.state
            .borrow_mut()
            .joined(coordinator, generation.clone());

        let client = self.client.clone();
        let handle = self.client.handle().clone();
        let state = self.state.clone();
        let heartbeat = self.timer
            .interval_at(Instant::now() + self.heartbeat_interval,
                         self.heartbeat_interval)
            .from_err()
            .for_each(move |_| {
                let client = client.clone();
                let state = state.clone();
                let generation = generation.clone();
                let matched = *state.borrow() ==
                              (State::Stable {
                                   coordinator: coordinator,
                                   generation: generation.clone(),
                               });

                if matched {
                    let send_heartbeat =
                        Retry::spawn(handle.clone(),
                                     client.retry_strategy(),
                                     move || client.heartbeat(coordinator, generation.clone()));

                    Either::A(send_heartbeat
                                  .from_err()
                                  .map_err(move |err| {
                        match err {
                            Error(ErrorKind::KafkaError(KafkaCode::GroupLoadInProgress), _) |
                            Error(ErrorKind::KafkaError(KafkaCode::RebalanceInProgress), _) => {
                                info!("group is loading or rebalancing, {}", err);

                                state.borrow_mut().rebalance(coordinator);
                            }
                            Error(ErrorKind::KafkaError(KafkaCode::GroupCoordinatorNotAvailable), _) |
                            Error(ErrorKind::KafkaError(KafkaCode::NotCoordinatorForGroup), _) |
                            Error(ErrorKind::KafkaError(KafkaCode::IllegalGeneration), _) |
                            Error(ErrorKind::KafkaError(KafkaCode::UnknownMemberId), _) => {
                                info!("group has outdated, need to rejoin, {}", err);

                                state.borrow_mut().leave();
                            }
                            _ => warn!("unknown"),
                        };

                        err
                    }))
                } else {
                    Either::B(future::err(ErrorKind::Canceled("group generation outdated").into()))
                }
            })
            .map_err(move |err| match err {
                         Error(ErrorKind::Canceled(reason), _) => {
                             trace!("heartbeat canceled, {}", reason);
                         }
                         _ => {
                             warn!("heartbeat failed, {}", err);
                         }
                     });

        self.client.handle().spawn(heartbeat);

        Ok(())
    }
}

impl<'a> Coordinator for ConsumerCoordinator<'a>
    where Self: 'static
{
    fn join_group(&mut self) -> JoinGroup {
        let inner = self.inner.clone();
        let client = self.inner.client.clone();
        let member_id = self.inner.state.borrow().member_id().unwrap_or_default();
        let group_id = self.inner.group_id.clone();
        let session_timeout = self.inner.session_timeout;
        let rebalance_timeout = self.inner.rebalance_timeout;
        let group_protocols = self.inner.group_protocols();
        let state = self.inner.state.clone();

        debug!("member `{}` is joining the `{}` group", member_id, group_id);

        let group_coordinator = match *state.borrow() {
            State::Stable { .. } => {
                return JoinGroup::new(future::ok(()));
            }
            State::Rebalancing { coordinator } => Either::A(future::ok(coordinator)),
            State::Unjoined => {
                Either::B(self.inner
                              .client
                              .group_coordinator(group_id.clone().into())
                              .map(|coordinator| coordinator.as_ref()))
            }
        };

        let future = self.inner
            .client
            .metadata()
            .join(group_coordinator)
            .and_then(move |(metadata, coordinator)| {
                client
                    .join_group(coordinator,
                                group_id.clone().into(),
                                session_timeout.as_millis() as i32,
                                rebalance_timeout.as_millis() as i32,
                                member_id.clone().into(),
                                CONSUMER_PROTOCOL.into(),
                                group_protocols)
                    .and_then(move |consumer_group| {
                        let generation = consumer_group.generation();

                        let group_assignment = if !consumer_group.is_leader() {
                            debug!("member `{}` joined group `{}` as follower",
                                   member_id,
                                   group_id);

                            None
                        } else {
                            debug!("member `{}` joined group `{}` as leader",
                                   member_id,
                                   group_id);

                            match inner.perform_assignment(&metadata,
                                                           &consumer_group.protocol,
                                                           &consumer_group.members) {
                                Ok(group_assignment) => Some(group_assignment),
                                Err(err) => return JoinGroup::err(err),
                            }
                        };

                        let future = client
                            .sync_group(coordinator, generation.clone(), group_assignment)
                            .and_then(move |assignment| {
                                          debug!("group `{}` synced up", group_id);

                                          inner.synced_group(Schema::deserialize(&assignment[..])
                                                                 .chain_err(||"fail to deserialize assignment")?,
                                                             coordinator,
                                                             generation)
                                      });

                        JoinGroup::new(future)
                    })
            })
            .map_err(move |err| {
                         warn!("fail to join group, {}", err);

                         state.borrow_mut().leave();

                         err
                     });

        JoinGroup::new(future)
    }

    fn leave_group(&mut self) -> LeaveGroup {
        let state = self.inner.state.borrow_mut().leave();

        if let State::Stable {
                   coordinator,
                   generation,
               } = state {
            let group_id = self.inner.group_id.clone();

            debug!("member `{}` is leaving the `{}` group",
                   generation.member_id,
                   group_id);

            LeaveGroup::new(self.inner
                                .client
                                .leave_group(coordinator, generation)
                                .map(|group_id| {
                                         debug!("member has leaved the `{}` group", group_id);
                                     }))
        } else {
            LeaveGroup::err(ErrorKind::KafkaError(KafkaCode::GroupLoadInProgress).into())
        }
    }
}

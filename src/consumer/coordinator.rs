use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::mem;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::{Future, Stream, future};
use futures::future::Either;
use tokio_retry::Retry;
use tokio_timer::Timer;

use client::{BrokerRef, Client, Cluster, ConsumerGroupAssignment, ConsumerGroupMember,
             ConsumerGroupProtocol, Generation, JoinGroup as JoinConsumerGroup, Metadata,
             OffsetCommit, OffsetFetch, StaticBoxFuture, ToStaticBoxFuture};
use consumer::{Assignment, CONSUMER_PROTOCOL, PartitionAssignor, Subscription, Subscriptions};
use errors::{Error, ErrorKind, Result, ResultExt};
use network::{OffsetAndMetadata, TopicPartition};
use protocol::{KafkaCode, Schema, ToMilliseconds};

/// Manages the coordination process with the consumer coordinator.
pub trait Coordinator<'a> {
    /// Discover the current coordinator for the group.
    fn group_coordinator(&self) -> GroupCoordinator;

    /// Join the consumer group.
    fn join_group(&self) -> JoinGroup;

    /// Rejoin the consumer group if need.
    fn rejoin_group(&self, member_id: Option<String>) -> RejoinGroup;

    // Ensure that the group is active (i.e. joined and synced)
    fn ensure_active_group(&self) -> ActiveGroup;

    /// Leave the current consumer group.
    fn leave_group(&self) -> LeaveGroup;

    /// Commit the specified offsets for the specified list of topics and partitions to Kafka.
    fn commit_offsets(&self) -> CommitOffset;

    /// Refresh the committed offsets for provided partitions.
    fn refresh_committed_offsets(&self) -> RefreshCommittedOffsets;

    /// Fetch the current committed offsets from the coordinator for a set of partitions.
    fn fetch_committed_offsets(&self, partitions: Vec<TopicPartition<'a>>)
        -> FetchCommittedOffsets;
}

pub type GroupCoordinator = StaticBoxFuture<BrokerRef>;

pub type JoinGroup = StaticBoxFuture<(BrokerRef, Generation)>;

pub type RejoinGroup = JoinGroup;

pub type ActiveGroup = JoinGroup;

pub type LeaveGroup = StaticBoxFuture;

pub type CommitOffset = OffsetCommit;

pub type RefreshCommittedOffsets = StaticBoxFuture;

pub type FetchCommittedOffsets = OffsetFetch;

/// Manages the coordination process with the consumer coordinator.
pub struct ConsumerCoordinator<'a, C> {
    inner: Rc<Inner<'a, C>>,
}

struct Inner<'a, C> {
    client: C,
    group_id: String,
    subscriptions: Rc<RefCell<Subscriptions<'a>>>,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    heartbeat_interval: Duration,
    retention_time: Option<Duration>,
    auto_commit_interval: Option<Duration>,
    assignors: Vec<Box<PartitionAssignor>>,
    state: Rc<RefCell<State>>,
    timer: Rc<Timer>,
}

#[derive(Clone, Debug, PartialEq)]
enum State {
    /// the client is not part of a group
    Unjoined,
    /// the client has begun rebalancing
    Rebalancing {
        coordinator: BrokerRef,
        generation: Generation,
    },
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

    pub fn rebalance(&mut self, coordinator: BrokerRef, generation: Generation) -> Self {
        mem::replace(
            self,
            State::Rebalancing {
                coordinator: coordinator,
                generation: generation,
            },
        )
    }

    pub fn joined(&mut self, coordinator: BrokerRef, generation: Generation) -> State {
        mem::replace(
            self,
            State::Stable {
                coordinator: coordinator,
                generation: generation,
            },
        )
    }

    pub fn leave(&mut self) -> Self {
        mem::replace(self, State::Unjoined)
    }
}

impl<'a, C> ConsumerCoordinator<'a, C> {
    pub fn new(
        client: C,
        group_id: String,
        subscriptions: Rc<RefCell<Subscriptions<'a>>>,
        session_timeout: Duration,
        rebalance_timeout: Duration,
        heartbeat_interval: Duration,
        retention_time: Option<Duration>,
        auto_commit_interval: Option<Duration>,
        assignors: Vec<Box<PartitionAssignor>>,
        timer: Rc<Timer>,
    ) -> Self {
        ConsumerCoordinator {
            inner: Rc::new(Inner {
                client: client,
                group_id: group_id,
                subscriptions: subscriptions,
                session_timeout: session_timeout,
                rebalance_timeout: rebalance_timeout,
                heartbeat_interval: heartbeat_interval,
                retention_time: retention_time,
                auto_commit_interval: auto_commit_interval,
                assignors: assignors,
                timer: timer,
                state: Rc::new(RefCell::new(State::Unjoined)),
            }),
        }
    }
}

impl<'a, C> Inner<'a, C>
where
    C: Client<'a> + Clone,
    Self: 'static,
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
                let subscription = assignor.subscription(
                    topics
                        .iter()
                        .map(|topic_name| topic_name.as_str().into())
                        .collect(),
                );

                Schema::serialize(&subscription)
                    .map_err(|err| {
                        warn!("fail to serialize subscription schema, {}", err)
                    })
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

    fn perform_assignment(
        &self,
        metadata: &Metadata,
        group_protocol: &str,
        members: &[ConsumerGroupMember],
    ) -> Result<Vec<ConsumerGroupAssignment<'a>>> {
        let strategy = group_protocol.parse().chain_err(|| {
            format!("fail to parse group protocol: {}", group_protocol)
        })?;
        let assignor = self.assignors
            .iter()
            .find(|assigner| assigner.strategy() == strategy)
            .ok_or_else(|| {
                ErrorKind::UnsupportedAssignmentStrategy(group_protocol.to_owned())
            })?;

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

        assigned_topics.extend(assignment.values().flat_map(|member| {
            member.partitions.iter().map(|tp| tp.topic_name.clone())
        }));

        let not_assigned_topics = &subscripbed_topics - &assigned_topics;

        if !not_assigned_topics.is_empty() {
            warn!(
                "The following subscribed topics are not assigned to any members in the group `{}`: {}",
                self.group_id,
                Vec::from_iter(not_assigned_topics.iter().cloned())
                    .as_slice()
                    .join(",")
            );
        }

        let newly_added_topics = &assigned_topics - &subscripbed_topics;

        if !newly_added_topics.is_empty() {
            info!(
                "The following not-subscribed topics are assigned to group {}, and their metadata will be fetched from the brokers : {}",
                self.group_id,
                Vec::from_iter(newly_added_topics.iter().cloned())
                    .as_slice()
                    .join(",")
            );

            subscripbed_topics.extend(assigned_topics);
        }

        self.subscriptions.borrow_mut().group_subscribe(
            subscripbed_topics.iter(),
        );

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

    fn synced_group(
        &self,
        assignment: Assignment<'a>,
        coordinator: BrokerRef,
        generation: Generation,
    ) -> Result<()> {
        trace!(
            "member `{}` synced up to generation # {} with {} partitions: {:?}",
            generation.member_id,
            generation.generation_id,
            assignment.partitions.len(),
            assignment.partitions
        );

        self.subscriptions
            .borrow_mut()
            .assign_from_subscribed(assignment.partitions)
            .chain_err(|| "fail to assign subscribed partitions")?;

        self.state.borrow_mut().joined(coordinator, generation);

        Ok(())
    }

    fn heartbeat(&self, coordinator: BrokerRef, generation: Generation) -> Result<()> {
        debug!(
            "send heartbeat to the group `{}` per {} seconds",
            generation.group_id,
            self.heartbeat_interval.as_secs()
        );

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

                let matched = *state.borrow() ==
                              (State::Stable {
                                   coordinator: coordinator,
                                   generation: generation.clone(),
                               });

                if matched {
                    let send_heartbeat = {
                        let generation = generation.clone();

                        Retry::spawn(handle.clone(),
                                     client.retry_strategy(),
                                     move || client.heartbeat(coordinator, generation.clone()))
                    };

                    let generation = generation.clone();

                    Either::A(send_heartbeat
                                  .from_err()
                                  .map_err(move |err| {
                        match err {
                            Error(ErrorKind::KafkaError(KafkaCode::GroupLoadInProgress), _) |
                            Error(ErrorKind::KafkaError(KafkaCode::RebalanceInProgress), _) => {
                                info!("group is loading or rebalancing, {}", err);

                                state.borrow_mut().rebalance(coordinator, generation.clone());
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

    fn group_coordinator(&self) -> GroupCoordinator {
        match *self.state.borrow() {
            State::Stable { coordinator, .. } |
            State::Rebalancing { coordinator, .. } => Either::A(future::ok(coordinator)),
            State::Unjoined => {
                Either::B(
                    self.client
                        .group_coordinator(self.group_id.clone().into())
                        .map(|coordinator| coordinator.as_ref()),
                )
            }
        }.static_boxed()
    }

    fn join_group(&self, coordinator: BrokerRef, member_id: Option<String>) -> JoinConsumerGroup {
        self.client.join_group(
            coordinator,
            self.group_id.clone().into(),
            self.session_timeout.as_millis() as i32,
            self.rebalance_timeout.as_millis() as i32,
            member_id.unwrap_or_default().into(),
            CONSUMER_PROTOCOL.into(),
            self.group_protocols(),
        )
    }
}

impl<'a, C> Coordinator<'a> for ConsumerCoordinator<'a, C>
where
    C: Client<'a> + Clone,
    Self: 'static,
{
    fn group_coordinator(&self) -> GroupCoordinator {
        self.inner.group_coordinator()
    }

    fn join_group(&self) -> JoinGroup {
        let group_id = self.inner.group_id.clone();

        debug!("coordinator is joining the `{}` group", group_id);

        let state = self.inner.state.clone();

        self.ensure_active_group()
            .map(|(coordinator, generation)| {
                info!(
                    "member `{}` joined the `{}` group ",
                    generation.member_id,
                    generation.group_id,
                );

                (coordinator, generation)
            })
            .map_err(move |err| {
                warn!("fail to join group `{}`, {}", group_id, err);

                state.borrow_mut().leave();

                err
            })
            .static_boxed()
    }

    fn ensure_active_group(&self) -> ActiveGroup {
        let group_id = self.inner.group_id.clone();

        let member_id = match *self.inner.state.borrow() {
            State::Stable {
                coordinator,
                ref generation,
            } => {
                debug!(
                    "member `{}` already in the `{}` group (generation #{})",
                    generation.member_id,
                    generation.group_id,
                    generation.generation_id
                );

                return future::ok((coordinator, generation.clone())).static_boxed();
            }
            State::Rebalancing { ref generation, .. } => {
                debug!(
                    "member `{}` is rebalancing in the `{}` group",
                    generation.member_id,
                    generation.group_id,
                );

                Some(generation.member_id.clone())
            }
            State::Unjoined => {
                debug!("member is joining the `{}` group", group_id);

                None
            }
        };

        self.rejoin_group(member_id)
    }

    fn rejoin_group(&self, member_id: Option<String>) -> RejoinGroup {
        let inner = self.inner.clone();
        let client = inner.client.clone();
        let group_id = inner.group_id.clone();

        client
            .metadata()
            .join(self.inner.group_coordinator())
            .and_then(move |(metadata, coordinator)| {
                debug!(
                    "coordinator of group `{}` @ {}",
                    group_id,
                    metadata.find_broker(coordinator).map_or(
                        coordinator
                            .index()
                            .to_string(),
                        |broker| {
                            format!("{}:{}", broker.host(), broker.port())
                        },
                    )
                );

                inner.join_group(coordinator, member_id).and_then(
                    move |consumer_group| {
                        let generation = consumer_group.generation();

                        let group_assignment = if !consumer_group.is_leader() {
                            debug!(
                                "member `{}` joined group `{}` as follower",
                                generation.member_id,
                                generation.group_id
                            );

                            None
                        } else {
                            debug!(
                                "member `{}` joined group `{}` as leader",
                                generation.member_id,
                                generation.group_id
                            );

                            match inner.perform_assignment(
                                &metadata,
                                &consumer_group.protocol,
                                &consumer_group.members,
                            ) {
                                Ok(group_assignment) => Some(group_assignment),
                                Err(err) => return future::err(err).static_boxed(),
                            }
                        };

                        client
                            .sync_group(coordinator, generation.clone(), group_assignment)
                            .and_then(move |assignment| {
                                debug!("group `{}` synced up", generation.group_id);

                                inner
                                    .synced_group(
                                        Schema::deserialize(&assignment[..]).chain_err(
                                            || "fail to deserialize assignment",
                                        )?,
                                        coordinator,
                                        generation.clone(),
                                    )
                                    .and_then(|_| inner.heartbeat(coordinator, generation.clone()))
                                    .map(|_| (coordinator, generation))
                            })
                            .static_boxed()
                    },
                )
            })
            .static_boxed()
    }

    fn leave_group(&self) -> LeaveGroup {
        let group_id = self.inner.group_id.clone();

        debug!("coordinator is leaving the `{}` group", group_id);

        let state = self.inner.state.clone();
        let state = state.borrow_mut().leave();

        match state {
            State::Stable {
                coordinator,
                generation,
            } => {
                let member_id = generation.member_id.clone();

                self.inner
                    .client
                    .leave_group(coordinator, generation)
                    .map(move |group_id| {
                        debug!("member `{}` has leaved the `{}` group", member_id, group_id);
                    })
                    .map_err(move |err| {
                        warn!("fail to leave the `{}` group, {}", group_id, err);

                        err
                    })
                    .static_boxed()
            }
            _ => ErrorKind::KafkaError(KafkaCode::GroupLoadInProgress).into(),
        }
    }

    fn commit_offsets(&self) -> CommitOffset {
        debug!("commit offsets to the `{}` group", self.inner.group_id);

        let client = self.inner.client.clone();
        let retention_time = self.inner.retention_time;
        let subscriptions = self.inner.subscriptions.clone();
        let consumed = subscriptions.borrow().consumed_partitions();

        self.ensure_active_group()
            .and_then(move |(coordinator, generation)| {
                client.offset_commit(coordinator, generation, retention_time, consumed)
            })
            .static_boxed()
    }

    fn refresh_committed_offsets(&self) -> RefreshCommittedOffsets {
        debug!(
            "refresh committed offsets of the `{}` group",
            self.inner.group_id
        );

        let subscriptions = self.inner.subscriptions.clone();

        self.fetch_committed_offsets(self.inner.subscriptions.borrow().assigned_partitions())
            .and_then(move |offsets| {
                for (topic_name, partitions) in offsets {
                    for partition in partitions {
                        let tp = topic_partition!(topic_name.clone(), partition.partition_id);

                        if let Some(state) = subscriptions.borrow_mut().assigned_state_mut(&tp) {
                            state.committed = Some(OffsetAndMetadata::with_metadata(
                                partition.offset,
                                partition.metadata,
                            ));
                        }
                    }
                }

                Ok(())
            })
            .static_boxed()
    }

    fn fetch_committed_offsets(
        &self,
        partitions: Vec<TopicPartition<'a>>,
    ) -> FetchCommittedOffsets {
        debug!(
            "fetch committed offsets of the `{}` group: {:?}",
            self.inner.group_id,
            partitions
        );

        let client = self.inner.client.clone();
        self.ensure_active_group()
            .and_then(move |(coordinator, generation)| {
                client.offset_fetch(coordinator, generation, partitions)
            })
            .static_boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use futures::Async;

    use super::*;
    use client::{Broker, MockClient};
    use consumer::{ConsumerConfig, OffsetResetStrategy};

    const TEST_GROUP_ID: &str = "test-group";

    lazy_static! {
        static ref TEST_NODE: Broker = Broker::new(0, "localhost", 9092);
    }

    fn build_coordinator<'a>(
        client: MockClient,
        config: ConsumerConfig,
    ) -> ConsumerCoordinator<'a, MockClient> {
        ConsumerCoordinator::new(
            client,
            TEST_GROUP_ID.to_owned(),
            Rc::new(RefCell::new(
                Subscriptions::new(OffsetResetStrategy::Earliest),
            )),
            config.session_timeout(),
            config.rebalance_timeout(),
            config.heartbeat_interval(),
            None,
            config.auto_commit_interval(),
            vec![],
            Rc::new(config.timer()),
        )
    }

    #[test]
    fn test_lookup_coordinator() {
        let coordinator = build_coordinator(MockClient::new(), ConsumerConfig::default());

        assert!(matches!(coordinator.group_coordinator().poll(),
                Err(Error(ErrorKind::KafkaError(KafkaCode::GroupCoordinatorNotAvailable), _))));

        let coordinator =
            build_coordinator(
                MockClient::with_metadata(Metadata::with_brokers(vec![TEST_NODE.clone()])),
                ConsumerConfig::default(),
            );

        let node = TEST_NODE.clone();

        assert!(matches!(coordinator.group_coordinator().poll(), Ok(Async::Ready(node))));
    }
}

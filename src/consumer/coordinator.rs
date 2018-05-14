use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::mem;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::future::Either;
use futures::{future, Future, Stream};
use tokio_retry::{Error as RetryError, Retry};
use tokio_timer::Timer;

use client::{BrokerRef, Client, Cluster, ConsumerGroupAssignment, ConsumerGroupMember, ConsumerGroupProtocol,
             Generation, JoinGroup as JoinConsumerGroup, Metadata, OffsetCommit, OffsetFetch, StaticBoxFuture,
             ToStaticBoxFuture};
use consumer::{Assignment, PartitionAssignor, Subscription, Subscriptions, CONSUMER_PROTOCOL};
use errors::{Error, ErrorKind, Result, ResultExt};
use network::{OffsetAndMetadata, TopicPartition};
use protocol::{KafkaCode, Schema, ToMilliseconds};

/// Manages the coordination process with the consumer coordinator.
pub trait Coordinator<'a> {
    fn group_id(&self) -> &str;

    /// Join the consumer group.
    fn join_group(&self) -> JoinGroup;

    /// Leave the current consumer group.
    fn leave_group(&self) -> LeaveGroup;

    /// Commit the specified offsets for the specified list of topics and
    /// partitions to Kafka.
    fn commit_offsets<I>(&self, offsets: I) -> CommitOffset
    where
        I: 'static + IntoIterator<Item = (TopicPartition<'a>, OffsetAndMetadata)>;

    /// Refresh the committed offsets for provided partitions.
    fn update_offsets(&self) -> UpdateOffsets;

    /// Fetch the current committed offsets from the coordinator for a set of
    /// partitions.
    fn fetch_offsets(&self, partitions: Vec<TopicPartition<'a>>) -> FetchOffsets;
}

pub type JoinGroup = StaticBoxFuture<(BrokerRef, Generation)>;

pub type LeaveGroup = StaticBoxFuture;

pub type CommitOffset = OffsetCommit;

pub type UpdateOffsets = StaticBoxFuture;

pub type FetchOffsets = OffsetFetch;

/// Manages the coordination process with the consumer coordinator.
#[derive(Clone)]
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
    pub fn is_stable(&self) -> bool {
        if let State::Stable { .. } = *self {
            true
        } else {
            false
        }
    }

    pub fn is_rebalancing(&self) -> bool {
        if let State::Rebalancing { .. } = *self {
            true
        } else {
            false
        }
    }

    pub fn is_unstable(&self) -> bool {
        if let State::Unjoined = *self {
            true
        } else {
            false
        }
    }

    pub fn member_id(&self) -> Option<String> {
        match *self {
            State::Stable { ref generation, .. } | State::Rebalancing { ref generation, .. } => {
                Some(generation.member_id.clone())
            }
            State::Unjoined => None,
        }
    }

    pub fn rebalancing(&mut self, coordinator: BrokerRef, generation: Generation) -> Self {
        mem::replace(
            self,
            State::Rebalancing {
                coordinator,
                generation,
            },
        )
    }

    pub fn joined(&mut self, coordinator: BrokerRef, generation: Generation) -> Self {
        mem::replace(
            self,
            State::Stable {
                coordinator,
                generation,
            },
        )
    }

    pub fn leaved(&mut self) -> Self {
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
                client,
                group_id,
                subscriptions,
                session_timeout,
                rebalance_timeout,
                heartbeat_interval,
                retention_time,
                auto_commit_interval,
                assignors,
                timer,
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
                let subscription =
                    assignor.subscription(topics.iter().map(|topic_name| topic_name.as_str().into()).collect());

                Schema::serialize(&subscription)
                    .chain_err(|| "fail to serialize subscription schema")
                    .ok()
                    .map(|metadata| ConsumerGroupProtocol {
                        protocol_name: assignor.name().into(),
                        protocol_metadata: metadata.into(),
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
        let strategy = group_protocol
            .parse()
            .chain_err(|| format!("fail to parse group protocol: {}", group_protocol))?;
        let assignor = self.assignors
            .iter()
            .find(|assigner| assigner.strategy() == strategy)
            .ok_or_else(|| ErrorKind::UnsupportedAssignmentStrategy(group_protocol.to_owned()))?;

        let mut subscripbed_topics = HashSet::new();
        let mut subscriptions = HashMap::new();

        for member in members {
            let subscription: Subscription = Schema::deserialize(member.member_metadata.as_ref())
                .chain_err(|| "fail to deserialize member metadata schema")?;

            subscripbed_topics.extend(subscription.topics.iter().cloned());
            subscriptions.insert(member.member_id.as_str().into(), subscription);
        }

        let assignment = assignor.assign(metadata, subscriptions);

        // user-customized assignor may have created some topics that are not in the
        // subscription list and assign their partitions to the members; in
        // this case we would like to update the leader's own metadata with the
        // newly added topics so that it will not trigger a subsequent
        // rebalance when these topics gets updated from metadata refresh.

        let mut assigned_topics = HashSet::new();

        assigned_topics.extend(
            assignment
                .values()
                .flat_map(|member| member.partitions.iter().map(|tp| tp.topic_name.clone())),
        );

        let not_assigned_topics = &subscripbed_topics - &assigned_topics;

        if !not_assigned_topics.is_empty() {
            warn!(
                "The following subscribed topics are not assigned to any members in the group `{}`: {}",
                self.group_id,
                Vec::from_iter(not_assigned_topics.iter().cloned()).as_slice().join(",")
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

    fn synced_group(&self, assignment: Assignment<'a>, coordinator: BrokerRef, generation: Generation) -> Result<()> {
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
        let state = self.state.clone();

        let heartbeat = self.timer
            .interval_at(Instant::now() + self.heartbeat_interval, self.heartbeat_interval)
            .from_err()
            .for_each(move |_| {
                let client = client.clone();
                let state = state.clone();

                let matched = *state.borrow() == (State::Stable {
                    coordinator,
                    generation: generation.clone(),
                });

                if matched {
                    let send_heartbeat = {
                        let generation = generation.clone();

                        Retry::spawn(client.retry_strategy(), move || {
                            client.heartbeat(coordinator, generation.clone())
                        })
                    };

                    let generation = generation.clone();

                    Either::A(send_heartbeat.map_err(move |err| {
                        match err {
                            RetryError::OperationError(ref err) => match *err {
                                Error(ErrorKind::KafkaError(KafkaCode::CoordinatorLoadInProgress), _)
                                | Error(ErrorKind::KafkaError(KafkaCode::RebalanceInProgress), _) => {
                                    info!("group is loading or rebalancing, {}", err);

                                    state.borrow_mut().rebalancing(coordinator, generation.clone());
                                }
                                Error(ErrorKind::KafkaError(KafkaCode::CoordinatorNotAvailable), _)
                                | Error(ErrorKind::KafkaError(KafkaCode::NotCoordinator), _)
                                | Error(ErrorKind::KafkaError(KafkaCode::IllegalGeneration), _)
                                | Error(ErrorKind::KafkaError(KafkaCode::UnknownMemberId), _) => {
                                    info!("group has outdated, need to rejoin, {}", err);

                                    state.borrow_mut().leaved();
                                }
                                _ => warn!("unknown error, {}", err),
                            },
                            RetryError::TimerError(_) => {}
                        }

                        err.into()
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
            State::Stable { coordinator, .. } | State::Rebalancing { coordinator, .. } => {
                Either::A(future::ok(coordinator))
            }
            State::Unjoined => Either::B(
                self.client
                    .group_coordinator(self.group_id.clone().into())
                    .map(|coordinator| coordinator.as_ref()),
            ),
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

impl<'a, C> ConsumerCoordinator<'a, C>
where
    C: Client<'a> + Clone,
    Self: 'static,
{
    pub fn is_stable(&self) -> bool {
        self.inner.state.borrow().is_stable()
    }

    pub fn is_rebalancing(&self) -> bool {
        self.inner.state.borrow().is_rebalancing()
    }

    pub fn is_unstable(&self) -> bool {
        self.inner.state.borrow().is_unstable()
    }

    /// Discover the current coordinator for the group.
    pub fn group_coordinator(&self) -> GroupCoordinator {
        self.inner.group_coordinator()
    }

    // Ensure that the group is active (i.e. joined and synced)
    fn ensure_active_group(&self) -> ActiveGroup {
        if let State::Stable {
            coordinator,
            ref generation,
        } = *self.inner.state.borrow()
        {
            debug!(
                "member `{}` already in the `{}` group (generation #{})",
                generation.member_id, generation.group_id, generation.generation_id
            );

            future::ok((coordinator, generation.clone())).static_boxed()
        } else {
            let member_id = self.inner.state.borrow().member_id();

            if let Some(ref member_id) = member_id {
                debug!("member `{}` rejoin the `{}` group", member_id, self.inner.group_id);
            } else {
                debug!("member join the `{}` group", self.inner.group_id);
            }

            self.rejoin_group(member_id)
        }
    }

    /// Rejoin the consumer group if need.
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
                        format!("broker#{}", coordinator.index()),
                        |broker| format!("{}:{}", broker.host(), broker.port())
                    )
                );

                inner
                    .join_group(coordinator, member_id)
                    .and_then(move |consumer_group| {
                        let generation = consumer_group.generation();

                        let group_assignment = if !consumer_group.is_leader() {
                            debug!(
                                "member `{}` joined group `{}` as follower",
                                generation.member_id, generation.group_id
                            );

                            None
                        } else {
                            debug!(
                                "member `{}` joined group `{}` as leader",
                                generation.member_id, generation.group_id
                            );

                            match inner.perform_assignment(&metadata, &consumer_group.protocol, &consumer_group.members)
                            {
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
                                        Schema::deserialize(&assignment[..])
                                            .chain_err(|| "fail to deserialize assignment")?,
                                        coordinator,
                                        generation.clone(),
                                    )
                                    .and_then(|_| inner.heartbeat(coordinator, generation.clone()))
                                    .map(|_| (coordinator, generation))
                            })
                            .static_boxed()
                    })
            })
            .static_boxed()
    }
}

pub type GroupCoordinator = StaticBoxFuture<BrokerRef>;

pub type RejoinGroup = JoinGroup;

pub type ActiveGroup = JoinGroup;

impl<'a, C> Coordinator<'a> for ConsumerCoordinator<'a, C>
where
    C: Client<'a> + Clone,
    Self: 'static,
{
    fn group_id(&self) -> &str {
        self.inner.group_id.as_str()
    }

    fn join_group(&self) -> JoinGroup {
        let group_id = self.inner.group_id.clone();

        debug!("coordinator is joining the `{}` group", group_id);

        let state = self.inner.state.clone();

        self.ensure_active_group()
            .map(|(coordinator, generation)| {
                info!(
                    "member `{}` joined the `{}` group ",
                    generation.member_id, generation.group_id,
                );

                (coordinator, generation)
            })
            .map_err(move |err| {
                warn!("fail to join group `{}`, {}", group_id, err);

                state.borrow_mut().leaved();

                err
            })
            .static_boxed()
    }

    fn leave_group(&self) -> LeaveGroup {
        let group_id = self.inner.group_id.clone();

        debug!("coordinator is leaving the `{}` group", group_id);

        let state = self.inner.state.clone();
        let state = state.borrow_mut().leaved();

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
            _ => ErrorKind::KafkaError(KafkaCode::CoordinatorLoadInProgress).into(),
        }
    }

    fn commit_offsets<I>(&self, offsets: I) -> CommitOffset
    where
        I: 'static + IntoIterator<Item = (TopicPartition<'a>, OffsetAndMetadata)>,
    {
        debug!("commit offsets to the `{}` group", self.inner.group_id);

        let client = self.inner.client.clone();
        let retention_time = self.inner.retention_time;

        self.ensure_active_group()
            .and_then(move |(coordinator, generation)| {
                client.offset_commit(Some(coordinator), Some(generation), retention_time, offsets)
            })
            .static_boxed()
    }

    fn update_offsets(&self) -> UpdateOffsets {
        debug!("refresh committed offsets of the `{}` group", self.inner.group_id);

        let subscriptions = self.inner.subscriptions.clone();

        self.fetch_offsets(self.inner.subscriptions.borrow().assigned_partitions())
            .and_then(move |offsets| {
                for (topic_name, partitions) in offsets {
                    for partition in partitions {
                        let tp = topic_partition!(topic_name.clone(), partition.partition_id);

                        if let Some(state) = subscriptions.borrow_mut().assigned_state_mut(&tp) {
                            state.committed =
                                Some(OffsetAndMetadata::with_metadata(partition.offset, partition.metadata));
                        }
                    }
                }

                Ok(())
            })
            .static_boxed()
    }

    fn fetch_offsets(&self, partitions: Vec<TopicPartition<'a>>) -> FetchOffsets {
        debug!(
            "fetch committed offsets of the `{}` group: {:?}",
            self.inner.group_id, partitions
        );

        let client = self.inner.client.clone();
        self.ensure_active_group()
            .and_then(move |(coordinator, generation)| client.offset_fetch(coordinator, generation, partitions))
            .static_boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::rc::Rc;

    use futures::Async;
    use tokio_core::reactor::Core;

    use super::*;
    use client::{self, Broker, ConsumerGroup, MockClient};
    use consumer::{AssignmentStrategy, ConsumerConfig, OffsetResetStrategy};

    const TEST_GROUP_ID: &str = "test-group";
    const TEST_PROTOCOL: &str = "dummy-subprotocol";
    const TEST_LEADER_ID: &str = "leader_id";
    const TEST_MEMBER_ID: &str = "member_id";

    lazy_static! {
        static ref TEST_NODE: Broker = Broker::new(0, "localhost", 9092);
        static ref TEST_GROUP: ConsumerGroup = ConsumerGroup {
            group_id: TEST_GROUP_ID.to_owned(),
            generation_id: 1,
            protocol: TEST_PROTOCOL.to_owned(),
            leader_id: TEST_LEADER_ID.to_owned(),
            member_id: TEST_MEMBER_ID.to_owned(),
            members: vec![],
        };
    }

    struct DummySubprotocol {}

    impl PartitionAssignor for DummySubprotocol {
        fn name(&self) -> &'static str {
            TEST_PROTOCOL
        }

        fn strategy(&self) -> AssignmentStrategy {
            AssignmentStrategy::Custom(TEST_PROTOCOL.to_owned())
        }

        fn assign<'a>(
            &self,
            _metadata: &'a Metadata,
            _subscriptions: HashMap<Cow<'a, str>, Subscription<'a>>,
        ) -> HashMap<Cow<'a, str>, Assignment<'a>> {
            HashMap::new()
        }
    }

    #[test]
    fn test_state() {
        let unjoined = State::Unjoined;

        let rebalancing = State::Rebalancing {
            coordinator: BrokerRef::new(0),
            generation: TEST_GROUP.generation(),
        };

        let stable = State::Stable {
            coordinator: BrokerRef::new(0),
            generation: TEST_GROUP.generation(),
        };

        let member_id = Some(TEST_MEMBER_ID.to_owned());

        assert_eq!(unjoined.member_id(), None);
        assert_eq!(rebalancing.member_id(), member_id);
        assert_eq!(stable.member_id(), member_id);

        let mut state = unjoined.clone();

        assert_eq!(state.rebalancing(BrokerRef::new(0), TEST_GROUP.generation()), unjoined);

        assert_eq!(state.joined(BrokerRef::new(0), TEST_GROUP.generation()), rebalancing);

        assert_eq!(state.leaved(), stable);
        assert_eq!(state, unjoined);
    }

    fn build_coordinator<'a>(
        client: MockClient<'a>,
        config: ConsumerConfig,
    ) -> ConsumerCoordinator<'a, MockClient<'a>> {
        ConsumerCoordinator::new(
            client,
            TEST_GROUP_ID.to_owned(),
            Rc::new(RefCell::new(Subscriptions::new(OffsetResetStrategy::Earliest))),
            config.session_timeout(),
            config.rebalance_timeout(),
            config.heartbeat_interval(),
            None,
            config.auto_commit_interval(),
            vec![Box::new(DummySubprotocol {})],
            Rc::new(config.timer()),
        )
    }

    #[test]
    fn test_lookup_coordinator() {
        let coordinator = build_coordinator(MockClient::new(), ConsumerConfig::default());

        match coordinator.group_coordinator().poll() {
            Err(Error(ErrorKind::KafkaError(KafkaCode::CoordinatorNotAvailable), _)) => {}
            res @ _ => panic!("fail to discover group coordinator: {:?}", res),
        }

        let node = TEST_NODE.clone();
        let coordinator = {
            let client = MockClient::with_metadata(Metadata::with_brokers(vec![node.clone()]))
                .with_group_coordinator(TEST_GROUP_ID.into(), node.clone());

            build_coordinator(client, ConsumerConfig::default())
        };

        match coordinator.group_coordinator().poll() {
            Ok(Async::Ready(group_coordinator)) => {
                assert_eq!(group_coordinator, node.as_ref());
            }
            res @ _ => panic!("fail to discover group coordinator: {:?}", res),
        }
    }

    #[test]
    fn test_join_group() {
        let node = TEST_NODE.clone();
        let group = TEST_GROUP.clone();
        let core = Core::new().unwrap();
        let client = MockClient::with_metadata(Metadata::with_brokers(vec![node.clone()]))
            .with_handle(core.handle())
            .with_group_coordinator(TEST_GROUP_ID.into(), node.clone())
            .with_consumer_group(group.clone())
            .with_group_member_as_follower(TEST_MEMBER_ID.into());
        let coordinator = build_coordinator(client, ConsumerConfig::default());

        assert!(coordinator.is_unstable());

        match coordinator.join_group().poll() {
            Ok(Async::Ready((group_coordinator, generation))) => {
                assert_eq!(group_coordinator, node.as_ref());
                assert_eq!(generation, group.generation());
            }
            res @ _ => panic!("fail to join group: {:?}", res),
        }

        assert!(coordinator.is_stable());
    }

    #[test]
    fn test_group_unauthorized() {
        let node = TEST_NODE.clone();
        let core = Core::new().unwrap();
        let client = MockClient::with_metadata(Metadata::with_brokers(vec![node.clone()]))
            .with_handle(core.handle())
            .with_future_response::<client::GroupCoordinator, _>(Box::new(|group_id| {
                assert_eq!(group_id, TEST_GROUP_ID.to_owned());

                bail!(ErrorKind::KafkaError(KafkaCode::GroupAuthorizationFailed))
            }));
        let coordinator = build_coordinator(client, ConsumerConfig::default());

        assert!(coordinator.is_unstable());

        match coordinator.join_group().poll() {
            Err(Error(ErrorKind::KafkaError(KafkaCode::GroupAuthorizationFailed), _)) => {}
            res @ _ => panic!("fail to fetch group coordinator: {:?}", res),
        }

        assert!(coordinator.is_unstable());
    }
}

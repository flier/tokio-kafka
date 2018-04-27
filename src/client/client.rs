use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::iter::{self, FromIterator};
use std::mem;
use std::cmp;
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::Deref;
use std::rc::Rc;
use std::time::Duration;
use std::usize;

use bytes::Bytes;

use rand::{self, Rng};

use futures::unsync::oneshot;
use futures::{future, Async, Future, IntoFuture, Poll};
use tokio_core::reactor::{Handle, Timeout};
use tokio_service::Service;
use tokio_timer::Timer;
use ns_router::{AutoName, Config as RouterConfig, Router, SubscribeExt};
use ns_std_threaded::ThreadedResolver;
use abstract_ns::HostResolve;

use client::middleware::Timeout as TimeoutMiddleware;
use client::{Broker, BrokerRef, ClientBuilder, ClientConfig, Cluster, FutureResponse, InFlightMiddleware,
             KafkaService, Metadata, Metrics};
use errors::{Error, Result};
use errors::ErrorKind::{self, *};
use network::{KafkaRequest, KafkaResponse, OffsetAndMetadata, TopicPartition, DEFAULT_PORT};
use protocol::{ApiKeys, ApiVersion, CorrelationId, ErrorCode, FetchOffset, FetchPartition, FetchTopic, FetchTopicData,
               GenerationId, IsolationLevel, JoinGroupMember, JoinGroupProtocol, KafkaCode, Message, MessageSet,
               Offset, PartitionId, RequiredAcks, SyncGroupAssignment, Timestamp, UsableApiVersions,
               DEFAULT_RESPONSE_MAX_BYTES};

/// A trait for communicating with the Kafka cluster.
pub trait Client<'a>: 'static {
    fn handle(&self) -> &Handle;

    fn metadata(&self) -> GetMetadata;

    /// The retry strategy when request failed
    fn retry_strategy(&self) -> Vec<Duration>;

    /// Send the given record asynchronously and return a future which will eventually contain
    /// the response information.
    fn produce_records(
        &self,
        acks: RequiredAcks,
        timeout: Duration,
        topic_partition: TopicPartition<'a>,
        records: Vec<Cow<'a, MessageSet>>,
    ) -> ProduceRecords;

    /// Fetch records of partitions for all nodes for which we have assigned
    /// partitions.
    fn fetch_records(
        &self,
        fetch_max_wait: Duration,
        fetch_min_bytes: usize,
        fetch_max_bytes: usize,
        isolation_level: IsolationLevel,
        partitions: Vec<(TopicPartition<'a>, PartitionData)>,
    ) -> FetchRecords;

    /// Search the offsets by target times for the specified topics and return a future which
    /// will eventually contain the partition offset information.
    fn list_offsets<I>(&self, partitions: I) -> ListOffsets
    where
        I: 'static + IntoIterator<Item = (TopicPartition<'a>, FetchOffset)>;

    /// Load metadata of the Kafka cluster and return a future which will eventually contain
    /// the metadata information.
    fn load_metadata(&mut self) -> LoadMetadata<'a>;

    /// Commit the specified offsets for the specified list of topics and
    /// partitions to Kafka.
    fn offset_commit<I>(
        &self,
        coordinator: Option<BrokerRef>,
        generation: Option<Generation>,
        retention_time: Option<Duration>,
        offsets: I,
    ) -> OffsetCommit
    where
        I: 'static + IntoIterator<Item = (TopicPartition<'a>, OffsetAndMetadata)>;

    /// Fetch the current committed offsets from the coordinator for a set of
    /// partitions.
    fn offset_fetch<I>(&self, coordinator: BrokerRef, generation: Generation, partitions: I) -> OffsetFetch
    where
        I: 'static + IntoIterator<Item = TopicPartition<'a>>;

    /// Discover the current coordinator of the consumer group.
    fn group_coordinator(&self, group_id: Cow<'a, str>) -> GroupCoordinator;

    /// Join the consumer group
    fn join_group(
        &self,
        coordinator: BrokerRef,
        group_id: Cow<'a, str>,
        session_timeout: i32,
        rebalance_timeout: i32,
        member_id: Cow<'a, str>,
        protocol_type: Cow<'a, str>,
        group_protocols: Vec<ConsumerGroupProtocol<'a>>,
    ) -> JoinGroup;

    /// Send heartbeat to the consumer group
    fn heartbeat(&self, coordinator: BrokerRef, generation: Generation) -> Heartbeat;

    /// Leave the current consumer group
    fn leave_group(&self, coordinator: BrokerRef, generation: Generation) -> LeaveGroup;

    /// Sync the current consumer group
    fn sync_group(
        &self,
        coordinator: BrokerRef,
        generation: Generation,
        group_assignment: Option<Vec<ConsumerGroupAssignment<'a>>>,
    ) -> SyncGroup;
}

/// The future of producing records.
pub type ProduceRecords = StaticBoxFuture<HashMap<String, Vec<ProducedRecords>>>;

/// Produced records of partition.
#[derive(Clone, Debug, PartialEq)]
pub struct ProducedRecords {
    /// The partition id
    pub partition_id: PartitionId,
    /// The error code
    pub error_code: KafkaCode,
    /// The offset found in the partition
    pub base_offset: Offset,
}

/// The future of fetch records of partitions.
pub type FetchRecords = StaticBoxFuture<(Duration, HashMap<String, Vec<FetchedRecords>>)>;

#[derive(Clone, Debug, PartialEq)]
pub struct FetchedRecords {
    /// The partition id
    pub partition_id: PartitionId,
    /// The error code
    pub error_code: KafkaCode,
    /// The offset found in the partition
    pub fetch_offset: Offset,
    /// The offset at the end of the log for this partition.
    pub high_watermark: Offset,
    /// The message data fetched from this partition, in the format described
    /// above.
    pub messages: Vec<Message>,
}

/// The future of partition offsets information.
pub type ListOffsets = StaticBoxFuture<HashMap<String, Vec<ListedOffset>>>;

/// The partition and offset
#[derive(Clone, Debug, PartialEq)]
pub struct ListedOffset {
    /// The partition id
    pub partition_id: PartitionId,
    /// The error code
    pub error_code: KafkaCode,
    /// The offset found in the partition
    pub offsets: Vec<Offset>,
    /// The timestamp associated with the returned offset
    pub timestamp: Option<Timestamp>,
}

impl ListedOffset {
    pub fn offset(&self) -> Option<Offset> {
        self.offsets.first().cloned()
    }

    pub fn earliest(&self) -> Option<Offset> {
        self.offsets.iter().cloned().min()
    }

    pub fn latest(&self) -> Option<Offset> {
        self.offsets.iter().cloned().max()
    }
}

/// The fetch partition data
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionData {
    /// Message offset.
    pub offset: Offset,
    /// Maximum bytes to fetch.
    pub max_bytes: Option<i32>,
}

pub type OffsetCommit = StaticBoxFuture<HashMap<String, Vec<CommittedOffset>>>;

#[derive(Clone, Debug, PartialEq)]
pub struct CommittedOffset {
    /// The partition id
    pub partition_id: PartitionId,
    /// The error code
    pub error_code: KafkaCode,
}

pub type OffsetFetch = StaticBoxFuture<HashMap<String, Vec<FetchedOffset>>>;

#[derive(Clone, Debug, PartialEq)]
pub struct FetchedOffset {
    /// The partition id
    pub partition_id: PartitionId,
    /// The error code
    pub error_code: KafkaCode,
    /// The offset found in the partition
    pub offset: Offset,
    /// Any associated metadata the client wants to keep.
    pub metadata: Option<String>,
}

/// The future of discover group coodinator
pub type GroupCoordinator = StaticBoxFuture<Broker>;

/// The future of join group.
pub type JoinGroup = StaticBoxFuture<ConsumerGroup>;

pub type ConsumerGroupProtocol<'a> = JoinGroupProtocol<'a>;

/// The future of heartbeat.
pub type Heartbeat = StaticBoxFuture;

/// The consumer group
#[derive(Clone, Debug)]
pub struct ConsumerGroup {
    /// The group id.
    pub group_id: String,

    /// The generation of the consumer group.
    pub generation_id: GenerationId,

    /// The group protocol selected by the coordinator
    pub protocol: String,

    /// The leader of the group
    pub leader_id: String,

    /// The consumer id assigned by the group coordinator.
    pub member_id: String,

    /// The members of the group
    pub members: Vec<ConsumerGroupMember>,
}

impl ConsumerGroup {
    pub fn is_leader(&self) -> bool {
        self.leader_id == self.member_id
    }

    pub fn generation(&self) -> Generation {
        Generation {
            group_id: self.group_id.clone(),
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            protocol: self.protocol.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Generation {
    /// The group id.
    pub group_id: String,

    /// The generation of the consumer group.
    pub generation_id: GenerationId,

    /// The consumer id assigned by the group coordinator.
    pub member_id: String,

    /// The group protocol selected by the coordinator
    pub protocol: String,
}

/// The consumer group member
pub type ConsumerGroupMember = JoinGroupMember;

/// The future of leave consumer group.
pub type LeaveGroup = StaticBoxFuture<String>;

pub type ConsumerGroupAssignment<'a> = SyncGroupAssignment<'a>;

/// The future of sync consumer group.
pub type SyncGroup = StaticBoxFuture<Bytes>;

/// A Kafka client that communicate with the Kafka cluster.
#[derive(Clone)]
pub struct KafkaClient<'a> {
    inner: Rc<Inner<'a>>,
}

struct Inner<'a> {
    config: ClientConfig,
    handle: Handle,
    service: Rc<InFlightMiddleware<TimeoutMiddleware<KafkaService<'a>>>>,
    timer: Rc<Timer>,
    router: Rc<Router>,
    metrics: Option<Rc<Metrics>>,
    state: Rc<RefCell<State>>,
}

#[derive(Default)]
struct State {
    correlation_id: CorrelationId,
    metadata_status: MetadataStatus,
}

enum MetadataStatus {
    Loading(RefCell<Vec<oneshot::Sender<Rc<Metadata>>>>),
    Loaded(Rc<Metadata>),
}

impl Default for MetadataStatus {
    fn default() -> Self {
        MetadataStatus::Loading(RefCell::new(Vec::new()))
    }
}

impl<'a> Deref for KafkaClient<'a> {
    type Target = ClientConfig;

    fn deref(&self) -> &Self::Target {
        &self.inner.config
    }
}

impl<'a> KafkaClient<'a>
where
    Self: 'static,
{
    pub fn new(config: ClientConfig, handle: Handle) -> KafkaClient<'a> {
        trace!("create client from config: {:?}", config);

        let metrics = if config.metrics {
            Some(Rc::new(Metrics::new().expect("fail to register metrics")))
        } else {
            None
        };
        let timer = Rc::new(config.timer());
        let router = Rc::new(Router::from_config(
            &RouterConfig::new()
                .set_fallthrough(
                    ThreadedResolver::new()
                        .null_service_resolver()
                        .interval_subscriber(Duration::new(1, 0), &handle),
                )
                .done(),
            &handle,
        ));
        let service = Rc::new(InFlightMiddleware::new(TimeoutMiddleware::new(
            KafkaService::new(
                handle.clone(),
                router.clone(),
                config.max_connection_idle(),
                metrics.clone(),
            ),
            config.timer(),
            config.request_timeout(),
        )));
        let inner = Rc::new(Inner {
            config,
            handle,
            service,
            timer,
            router,
            metrics,
            state: Rc::new(RefCell::new(State::default())),
        });

        let mut client = KafkaClient { inner };

        client.refresh_metadata();

        client
    }

    /// Construct a `ClientBuilder` from ClientConfig
    pub fn with_config(config: ClientConfig, handle: Handle) -> ClientBuilder<'a> {
        ClientBuilder::with_config(config, handle)
    }

    /// Construct a `ClientBuilder` from  bootstrap servers of the Kafka cluster
    pub fn with_bootstrap_servers<I>(hosts: I, handle: Handle) -> ClientBuilder<'a>
    where
        I: IntoIterator<Item = String>,
    {
        ClientBuilder::with_bootstrap_servers(hosts, handle)
    }

    pub fn timer(&self) -> Rc<Timer> {
        self.inner.timer.clone()
    }

    pub fn metrics(&self) -> Option<Rc<Metrics>> {
        self.inner.metrics.clone()
    }

    pub fn refresh_metadata(&mut self) {
        let handle = self.inner.handle.clone();

        handle.spawn(
            self.load_metadata()
                .map(|metadata| {
                    trace!("auto loaded metadata, {:?}", metadata);
                })
                .map_err(|err| {
                    warn!("fail to load metadata, {}", err);
                }),
        );
    }
}

pub enum GetMetadata {
    Loaded(Rc<Metadata>),
    Loading(oneshot::Receiver<Rc<Metadata>>),
}

impl Future for GetMetadata {
    type Item = Rc<Metadata>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            GetMetadata::Loaded(ref meta) => Ok(Async::Ready(meta.clone())),
            GetMetadata::Loading(ref mut inner) => inner.poll().map_err(|_| Canceled("load metadata canceled").into()),
        }
    }
}

impl<'a> Client<'a> for KafkaClient<'a>
where
    Self: 'static,
{
    fn handle(&self) -> &Handle {
        &self.inner.handle
    }

    fn metadata(&self) -> GetMetadata {
        (*self.inner.state).borrow().metadata()
    }

    fn retry_strategy(&self) -> Vec<Duration> {
        self.inner.config.retry_strategy()
    }

    fn produce_records(
        &self,
        required_acks: RequiredAcks,
        timeout: Duration,
        tp: TopicPartition<'a>,
        records: Vec<Cow<'a, MessageSet>>,
    ) -> ProduceRecords {
        let inner = self.inner.clone();
        self.metadata()
            .and_then(move |metadata| inner.produce_records(&metadata, required_acks, timeout, &tp, records))
            .static_boxed()
    }

    fn fetch_records(
        &self,
        fetch_max_wait: Duration,
        fetch_min_bytes: usize,
        fetch_max_bytes: usize,
        isolation_level: IsolationLevel,
        partitions: Vec<(TopicPartition<'a>, PartitionData)>,
    ) -> FetchRecords {
        let inner = self.inner.clone();
        self.metadata()
            .and_then(move |metadata| {
                inner
                    .topics_by_broker(ApiKeys::Fetch, &metadata, partitions)
                    .into_future()
                    .and_then(move |topics| {
                        inner.fetch_records(
                            fetch_max_wait,
                            fetch_min_bytes,
                            fetch_max_bytes,
                            isolation_level,
                            topics,
                        )
                    })
            })
            .static_boxed()
    }

    fn list_offsets<I>(&self, partitions: I) -> ListOffsets
    where
        I: 'static + IntoIterator<Item = (TopicPartition<'a>, FetchOffset)>,
    {
        let inner = self.inner.clone();
        self.metadata()
            .and_then(move |metadata| {
                inner
                    .topics_by_broker(ApiKeys::ListOffsets, &metadata, partitions)
                    .into_future()
                    .and_then(move |topics| inner.list_offsets(topics))
            })
            .static_boxed()
    }

    fn load_metadata(&mut self) -> LoadMetadata<'a> {
        if self.inner.config.metadata_max_age > 0 {
            let handle = self.inner.handle.clone();

            let timeout = Timeout::new(self.inner.config.metadata_max_age(), &handle);

            match timeout {
                Ok(timeout) => {
                    let inner = self.inner.clone();
                    let future = timeout
                        .from_err()
                        .and_then(move |_| LoadMetadata::new(inner.clone()))
                        .map(|_| ())
                        .map_err(|_| ());

                    handle.spawn(future);
                }
                Err(err) => {
                    warn!("fail to create timeout, {}", err);
                }
            }
        }

        LoadMetadata::new(self.inner.clone())
    }

    fn offset_commit<I>(
        &self,
        coordinator: Option<BrokerRef>,
        generation: Option<Generation>,
        retention_time: Option<Duration>,
        offsets: I,
    ) -> OffsetCommit
    where
        I: 'static + IntoIterator<Item = (TopicPartition<'a>, OffsetAndMetadata)>,
    {
        let inner = self.inner.clone();
        self.metadata()
            .and_then(move |metadata| {
                let broker = if let Some(coordinator) = coordinator {
                    metadata.find_broker(coordinator)
                } else {
                    metadata.brokers().first()
                };

                broker
                    .map(move |coordinator| {
                        inner.offset_commit(
                            coordinator,
                            generation.as_ref().map(|generation| generation.group_id.clone().into()),
                            generation.as_ref().map(|generation| generation.generation_id),
                            generation
                                .as_ref()
                                .map(|generation| generation.member_id.clone().into()),
                            retention_time,
                            offsets,
                        )
                    })
                    .unwrap_or_else(|| BrokerNotFound(coordinator.unwrap_or_default()).into())
            })
            .static_boxed()
    }

    fn offset_fetch<I>(&self, coordinator: BrokerRef, generation: Generation, partitions: I) -> OffsetFetch
    where
        I: 'static + IntoIterator<Item = TopicPartition<'a>>,
    {
        let inner = self.inner.clone();
        self.metadata()
            .and_then(move |metadata| {
                metadata
                    .find_broker(coordinator)
                    .map(move |coordinator| inner.offset_fetch(coordinator, generation.group_id.into(), partitions))
                    .unwrap_or_else(|| BrokerNotFound(coordinator).into())
            })
            .static_boxed()
    }

    fn group_coordinator(&self, group_id: Cow<'a, str>) -> GroupCoordinator {
        let inner = self.inner.clone();
        self.metadata()
            .and_then(move |metadata| inner.group_coordinator(&metadata, group_id))
            .static_boxed()
    }

    fn join_group(
        &self,
        coordinator: BrokerRef,
        group_id: Cow<'a, str>,
        session_timeout: i32,
        rebalance_timeout: i32,
        member_id: Cow<'a, str>,
        protocol_type: Cow<'a, str>,
        group_protocols: Vec<ConsumerGroupProtocol<'a>>,
    ) -> JoinGroup {
        let inner = self.inner.clone();
        self.metadata()
            .and_then(move |metadata| {
                metadata
                    .find_broker(coordinator)
                    .map(move |coordinator| {
                        inner.join_group(
                            coordinator,
                            group_id,
                            session_timeout,
                            rebalance_timeout,
                            member_id,
                            protocol_type,
                            group_protocols,
                        )
                    })
                    .unwrap_or_else(|| BrokerNotFound(coordinator).into())
            })
            .static_boxed()
    }

    fn heartbeat(&self, coordinator: BrokerRef, generation: Generation) -> Heartbeat {
        let inner = self.inner.clone();
        self.metadata()
            .and_then(move |metadata| {
                metadata
                    .find_broker(coordinator)
                    .map(move |coordinator| {
                        inner.heartbeat(
                            coordinator,
                            generation.group_id.into(),
                            generation.generation_id,
                            generation.member_id.into(),
                        )
                    })
                    .unwrap_or_else(|| BrokerNotFound(coordinator).into())
            })
            .static_boxed()
    }

    fn leave_group(&self, coordinator: BrokerRef, generation: Generation) -> LeaveGroup {
        let inner = self.inner.clone();
        self.metadata()
            .and_then(move |metadata| {
                metadata
                    .find_broker(coordinator)
                    .map(move |coordinator| {
                        inner.leave_group(coordinator, generation.group_id.into(), generation.member_id.into())
                    })
                    .unwrap_or_else(|| BrokerNotFound(coordinator).into())
            })
            .static_boxed()
    }

    fn sync_group(
        &self,
        coordinator: BrokerRef,
        generation: Generation,
        group_assignment: Option<Vec<ConsumerGroupAssignment<'a>>>,
    ) -> SyncGroup {
        let inner = self.inner.clone();
        self.metadata()
            .and_then(move |metadata| {
                metadata
                    .find_broker(coordinator)
                    .map(move |coordinator| {
                        inner.sync_group(
                            coordinator,
                            generation.group_id.into(),
                            generation.generation_id,
                            generation.member_id.into(),
                            group_assignment,
                        )
                    })
                    .unwrap_or_else(|| BrokerNotFound(coordinator).into())
            })
            .static_boxed()
    }
}

impl<'a> Inner<'a>
where
    Self: 'static,
{
    fn next_correlation_id(&self) -> CorrelationId {
        (*self.state).borrow_mut().next_correlation_id()
    }

    fn client_id(&self) -> Option<Cow<'a, str>> {
        self.config.client_id.clone().map(Cow::from)
    }

    pub fn metadata(&self) -> GetMetadata {
        (*self.state).borrow().metadata()
    }

    fn send_request<'n, N>(&self, host: N, req: KafkaRequest<'a>) -> FutureResponse
    where
        N: Into<AutoName<'n>>,
    {
        let service = self.service.clone();
        self.router
            .resolve_auto(host, DEFAULT_PORT)
            .from_err()
            .map(|addrs| addrs.pick_one().unwrap())
            .and_then(move |addr| service.call((addr, req)))
            .static_boxed()
    }

    /// Choose the node with the fewest outstanding requests which is at least eligible for
    /// connection.
    pub fn least_loaded_broker(&self, metadata: &Metadata) -> Result<(SocketAddr, BrokerRef)> {
        let mut brokers = metadata.brokers().to_vec();

        trace!("choose least broker from: {:?}", brokers);

        rand::thread_rng().shuffle(&mut brokers);

        let mut in_flight_requests = usize::MAX;
        let mut found = None;

        for broker in brokers {
            for addr in broker.addr().to_socket_addrs()? {
                match self.service.in_flight_requests(&addr) {
                    Some(0) => {
                        trace!(
                            "found least loaded broker #{} @ {} without in flight requests",
                            broker.id(),
                            addr
                        );

                        return Ok((addr, broker.as_ref()));
                    }
                    Some(n) if n < in_flight_requests => {
                        in_flight_requests = n;
                        found = Some((addr, broker.as_ref()));
                    }
                    _ => {}
                }
            }
        }

        found
            .map(|(addr, broker)| {
                trace!(
                    "found least loaded broker #{} @ {} with {} in flight requests",
                    broker.index(),
                    addr,
                    self.service.in_flight_requests(&addr).unwrap_or_default()
                );

                (addr, broker)
            })
            .or_else(|| {
                metadata.brokers().first().map(|broker| {
                    let addr = broker.addr().to_socket_addrs().unwrap().next().unwrap();

                    trace!(
                        "not found any alive broker, use a random broker # {} @ {}",
                        broker.id(),
                        addr
                    );

                    (addr, broker.as_ref())
                })
            })
            .ok_or_else(|| {
                warn!("not found any broker");

                KafkaError(KafkaCode::BrokerNotAvailable).into()
            })
    }

    fn fetch_all_metadata(&self) -> FetchMetadata {
        self.fetch_metadata(iter::empty::<String>())
    }

    fn fetch_metadata<I, S>(&self, topic_names: I) -> FetchMetadata
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let topic_names = topic_names.into_iter().map(|s| s.into()).collect::<Vec<_>>();

        if topic_names.is_empty() {
            info!("fetch metadata for all toipcs");
        } else {
            info!("fetch metadata for toipcs: {:?}", topic_names);
        }

        let responses = {
            let mut responses = Vec::new();

            for host in &self.config.hosts {
                let request = KafkaRequest::fetch_metadata(
                    0, // api_version
                    self.next_correlation_id(),
                    self.client_id(),
                    &topic_names,
                );

                let response = self.send_request(host.as_str(), request).and_then(|res| {
                    if let KafkaResponse::Metadata(res) = res {
                        Ok(Rc::new(Metadata::from(res)))
                    } else {
                        bail!(UnexpectedResponse(res.api_key()))
                    }
                });

                responses.push(response);
            }

            responses
        };

        future::select_ok(responses)
            .map(|(metadata, _)| metadata)
            .static_boxed()
    }

    fn fetch_api_versions(&self, broker: &Broker) -> FetchApiVersions {
        debug!("fetch API versions for broker: {:?}", broker);

        let request = KafkaRequest::api_versions(self.next_correlation_id(), self.client_id());

        self.send_request(AutoName::HostPort(broker.host(), broker.port()), request)
            .and_then(|res| {
                if let KafkaResponse::ApiVersions(res) = res {
                    Ok(UsableApiVersions::new(res.api_versions))
                } else {
                    bail!(UnexpectedResponse(res.api_key()))
                }
            })
            .static_boxed()
    }

    fn load_api_versions(&self, metadata: &Metadata) -> LoadApiVersions {
        trace!("load API versions from brokers, {:?}", metadata.brokers());

        let responses = {
            let mut responses = Vec::new();

            for broker in metadata.brokers() {
                let broker_ref = broker.as_ref();
                let response = self.fetch_api_versions(broker)
                    .map(move |api_versions| (broker_ref, api_versions));

                responses.push(response);
            }

            responses
        };

        future::join_all(responses).map(HashMap::from_iter).static_boxed()
    }

    fn produce_records(
        &self,
        metadata: &Metadata,
        required_acks: RequiredAcks,
        timeout: Duration,
        tp: &TopicPartition<'a>,
        records: Vec<Cow<'a, MessageSet>>,
    ) -> ProduceRecords {
        let (api_version, addr) = metadata.leader_for(tp).map_or_else(
            || (0, AutoName::Auto(self.config.hosts.first().unwrap())),
            |broker| {
                (
                    broker.api_version(ApiKeys::Produce).unwrap_or_default(),
                    AutoName::HostPort(broker.host(), broker.port()),
                )
            },
        );

        let request = KafkaRequest::produce_records(
            api_version,
            self.next_correlation_id(),
            self.client_id(),
            None,
            required_acks,
            timeout,
            tp,
            records,
        );

        self.send_request(addr, request)
            .and_then(|res| {
                if let KafkaResponse::Produce(res) = res {
                    Ok(res.topics)
                } else {
                    bail!(UnexpectedResponse(res.api_key()))
                }
            })
            .map(|topics| {
                topics
                    .into_iter()
                    .map(|topic| {
                        (
                            topic.topic_name.to_owned(),
                            topic
                                .partitions
                                .into_iter()
                                .map(|partition| ProducedRecords {
                                    partition_id: partition.partition_id,
                                    error_code: partition.error_code.into(),
                                    base_offset: partition.offset,
                                })
                                .collect(),
                        )
                    })
                    .collect()
            })
            .static_boxed()
    }

    fn topics_by_broker<I, T>(
        &self,
        api_key: ApiKeys,
        metadata: &Metadata,
        partitions: I,
    ) -> Result<TopicsByBroker<'a, T>>
    where
        I: IntoIterator<Item = (TopicPartition<'a>, T)>,
    {
        let mut topics = HashMap::new();

        for (tp, value) in partitions {
            let broker = metadata
                .leader_for(&tp)
                .ok_or_else(|| KafkaError(KafkaCode::NotLeaderForPartition))?;
            let api_version = broker.api_version(api_key).unwrap_or_default();

            topics
                .entry(((broker.host().to_owned(), broker.port()), api_version))
                .or_insert_with(HashMap::new)
                .entry(tp.topic_name)
                .or_insert_with(Vec::new)
                .push((tp.partition_id, value));
        }

        Ok(topics)
    }

    fn fetch_records(
        &self,
        fetch_max_wait: Duration,
        fetch_min_bytes: usize,
        fetch_max_bytes: usize,
        isolation_level: IsolationLevel,
        topics: TopicsByBroker<'a, PartitionData>,
    ) -> FetchRecords {
        let requests = {
            let mut requests = Vec::new();

            for (((host, port), api_version), offsets_by_topic) in topics {
                let fetch_topics = offsets_by_topic
                    .iter()
                    .map(|(topic_name, partitions)| FetchTopic {
                        topic_name: topic_name.to_owned(),
                        partitions: partitions
                            .iter()
                            .map(|&(partition_id, ref fetch_data)| FetchPartition {
                                partition_id,
                                fetch_offset: fetch_data.offset,
                                log_start_offset: None,
                                max_bytes: fetch_data.max_bytes.unwrap_or(DEFAULT_RESPONSE_MAX_BYTES),
                            })
                            .collect(),
                    })
                    .collect();

                let request = KafkaRequest::fetch_records(
                    api_version,
                    self.next_correlation_id(),
                    self.client_id(),
                    fetch_max_wait,
                    fetch_min_bytes as i32,
                    fetch_max_bytes as i32,
                    isolation_level,
                    fetch_topics,
                );
                let request = self.send_request(AutoName::HostPort(&host, port), request)
                    .and_then(|res| {
                        if let KafkaResponse::Fetch(res) = res {
                            Ok((res.throttle_time, res.topics))
                        } else {
                            bail!(UnexpectedResponse(res.api_key()))
                        }
                    })
                    .map(|(throttle_time, topics)| {
                        (
                            Duration::from_millis(throttle_time.unwrap_or_default() as u64),
                            Self::extract_fetched_records(offsets_by_topic, topics),
                        )
                    });

                requests.push(request);
            }

            requests
        };

        future::join_all(requests)
            .map(|responses| {
                responses.into_iter().fold(
                    (Duration::default(), HashMap::new()),
                    |(max_throttle_time, mut records), (throttle_time, response)| {
                        for (topic_name, mut partitions) in response {
                            records
                                .entry(topic_name)
                                .or_insert_with(Vec::new)
                                .append(&mut partitions);
                        }

                        (cmp::max(max_throttle_time, throttle_time), records)
                    },
                )
            })
            .static_boxed()
    }

    fn extract_fetched_records(
        offsets_by_topic: HashMap<Cow<'a, str>, Vec<(PartitionId, PartitionData)>>,
        topics: Vec<FetchTopicData>,
    ) -> Vec<(String, Vec<FetchedRecords>)> {
        topics
            .into_iter()
            .map(move |topic| {
                let topic_name = topic.topic_name;

                let offsets_by_topic_partition = {
                    let topic_name = Cow::from(topic_name.as_str());

                    offsets_by_topic
                        .get(&topic_name)
                        .map(move |offsets| {
                            offsets
                                .iter()
                                .map(|&(partition_id, ref fetch_data)| {
                                    let tp = topic_partition!(topic_name.clone(), partition_id);

                                    (tp, fetch_data)
                                })
                                .collect::<HashMap<TopicPartition, &PartitionData>>()
                        })
                        .unwrap_or_default()
                };

                let records = {
                    let topic_name = Cow::from(topic_name.as_str());

                    topic
                        .partitions
                        .into_iter()
                        .flat_map(move |data| {
                            let tp = topic_partition!(topic_name.clone(), data.partition_id);

                            offsets_by_topic_partition.get(&tp).map(move |&fetch| FetchedRecords {
                                partition_id: data.partition_id,
                                error_code: data.error_code.into(),
                                fetch_offset: fetch.offset,
                                high_watermark: data.high_watermark,
                                messages: data.message_set.messages,
                            })
                        })
                        .collect()
                };

                (topic_name.clone(), records)
            })
            .collect()
    }

    fn list_offsets(&self, topics: TopicsByBroker<'a, FetchOffset>) -> ListOffsets {
        debug!("list offsets of topics: {:?}", topics);

        let requests = {
            let mut requests = Vec::new();

            for (((host, port), api_version), topics) in topics {
                let request =
                    KafkaRequest::list_offsets(api_version, self.next_correlation_id(), self.client_id(), topics);
                let request = self.send_request(AutoName::HostPort(&host, port), request)
                    .and_then(|res| {
                        if let KafkaResponse::ListOffsets(res) = res {
                            Ok(res.topics)
                        } else {
                            bail!(UnexpectedResponse(res.api_key()))
                        }
                    })
                    .map(|topics| {
                        topics
                            .into_iter()
                            .map(|topic| {
                                let mut partitions = topic
                                    .partitions
                                    .into_iter()
                                    .map(|partition| ListedOffset {
                                        partition_id: partition.partition_id,
                                        error_code: partition.error_code.into(),
                                        offsets: partition.offsets,
                                        timestamp: partition.timestamp,
                                    })
                                    .collect::<Vec<_>>();

                                partitions.sort_by_key(|partition| partition.partition_id);

                                (topic.topic_name.clone(), partitions)
                            })
                            .collect::<Vec<(String, Vec<ListedOffset>)>>()
                    });

                requests.push(request);
            }

            requests
        };

        future::join_all(requests)
            .map(|responses| {
                responses.into_iter().fold(HashMap::new(), |mut offsets, response| {
                    for (topic_name, mut partitions) in response {
                        offsets
                            .entry(topic_name)
                            .or_insert_with(Vec::new)
                            .append(&mut partitions)
                    }
                    offsets
                })
            })
            .static_boxed()
    }

    fn offset_commit<I>(
        &self,
        coordinator: &Broker,
        group_id: Option<Cow<'a, str>>,
        group_generation_id: Option<GenerationId>,
        member_id: Option<Cow<'a, str>>,
        retention_time: Option<Duration>,
        offsets: I,
    ) -> OffsetCommit
    where
        I: IntoIterator<Item = (TopicPartition<'a>, OffsetAndMetadata)>,
    {
        debug!("commit offsets to the `{:?}` group", group_id);

        let addr = AutoName::HostPort(coordinator.host(), coordinator.port());

        let api_version = coordinator.api_version(ApiKeys::OffsetCommit).unwrap_or_default();

        let request = KafkaRequest::offset_commit(
            api_version,
            self.next_correlation_id(),
            self.client_id(),
            group_id,
            group_generation_id,
            member_id,
            retention_time,
            offsets,
        );

        self.send_request(addr, request)
            .and_then(|res| {
                if let KafkaResponse::OffsetCommit(res) = res {
                    Ok(res.topics)
                } else {
                    bail!(UnexpectedResponse(res.api_key()))
                }
            })
            .map(|topics| {
                topics
                    .into_iter()
                    .map(|status| {
                        let partitions = status
                            .partitions
                            .into_iter()
                            .map(|partition| CommittedOffset {
                                partition_id: partition.partition_id,
                                error_code: partition.error_code.into(),
                            })
                            .collect();

                        (status.topic_name, partitions)
                    })
                    .collect()
            })
            .static_boxed()
    }

    fn offset_fetch<I>(&self, coordinator: &Broker, group_id: Cow<'a, str>, partitions: I) -> OffsetFetch
    where
        I: IntoIterator<Item = TopicPartition<'a>>,
    {
        debug!("fetch offset of the `{:?}` group", group_id);

        let addr = AutoName::HostPort(coordinator.host(), coordinator.port());

        let api_version = coordinator.api_version(ApiKeys::OffsetFetch).unwrap_or_default();

        let request = KafkaRequest::offset_fetch(
            api_version,
            self.next_correlation_id(),
            self.client_id(),
            group_id,
            partitions,
        );

        self.send_request(addr, request)
            .and_then(|res| {
                if let KafkaResponse::OffsetFetch(res) = res {
                    Ok(res.topics)
                } else {
                    bail!(UnexpectedResponse(res.api_key()))
                }
            })
            .map(|topics| {
                topics
                    .into_iter()
                    .map(|status| {
                        let partitions = status
                            .partitions
                            .into_iter()
                            .map(|partition| FetchedOffset {
                                partition_id: partition.partition_id,
                                offset: partition.offset,
                                metadata: partition.metadata,
                                error_code: partition.error_code.into(),
                            })
                            .collect();

                        (status.topic_name, partitions)
                    })
                    .collect()
            })
            .static_boxed()
    }

    fn group_coordinator(&self, metadata: &Metadata, group_id: Cow<'a, str>) -> GroupCoordinator {
        debug!("disover group coordinator of group `{}`", group_id);

        let addr = {
            match self.least_loaded_broker(metadata) {
                Ok((addr, _)) => addr,
                Err(err) => {
                    return err.into();
                }
            }
        };

        let request = KafkaRequest::group_coordinator(
            0, // api_version,
            self.next_correlation_id(),
            self.client_id(),
            group_id,
        );

        self.service
            .call((addr, request))
            .and_then(|res| {
                if let KafkaResponse::GroupCoordinator(res) = res {
                    Ok(res)
                } else {
                    bail!(UnexpectedResponse(res.api_key()))
                }
            })
            .and_then(|res| {
                if res.error_code == KafkaCode::None as ErrorCode {
                    Ok(Broker::new(
                        res.coordinator_id,
                        &res.coordinator_host,
                        res.coordinator_port as u16,
                    ))
                } else {
                    bail!(KafkaError(res.error_code.into()))
                }
            })
            .static_boxed()
    }

    fn join_group(
        &self,
        coordinator: &Broker,
        group_id: Cow<'a, str>,
        session_timeout: i32,
        rebalance_timeout: i32,
        member_id: Cow<'a, str>,
        protocol_type: Cow<'a, str>,
        group_protocols: Vec<ConsumerGroupProtocol<'a>>,
    ) -> JoinGroup {
        if member_id.is_empty() {
            debug!("new member join the `{}` group", group_id);
        } else {
            debug!("member `{}` rejoin the `{}` group", member_id, group_id);
        }

        let addr = AutoName::HostPort(coordinator.host(), coordinator.port());

        let api_version = coordinator.api_version(ApiKeys::JoinGroup).unwrap_or_default();

        let joined_group_id: String = (*group_id).to_owned();

        let request = KafkaRequest::join_group(
            api_version,
            self.next_correlation_id(),
            self.client_id(),
            group_id,
            session_timeout,
            rebalance_timeout,
            member_id,
            protocol_type,
            group_protocols,
        );

        self.send_request(addr, request)
            .and_then(|res| {
                if let KafkaResponse::JoinGroup(res) = res {
                    Ok(res)
                } else {
                    bail!(UnexpectedResponse(res.api_key()))
                }
            })
            .and_then(move |res| {
                if res.error_code == KafkaCode::None as ErrorCode {
                    Ok(ConsumerGroup {
                        group_id: joined_group_id,
                        generation_id: res.generation_id,
                        protocol: res.protocol,
                        leader_id: res.leader_id,
                        member_id: res.member_id,
                        members: res.members,
                    })
                } else {
                    bail!(KafkaError(res.error_code.into()))
                }
            })
            .static_boxed()
    }

    fn heartbeat(
        &self,
        coordinator: &Broker,
        group_id: Cow<'a, str>,
        group_generation_id: GenerationId,
        member_id: Cow<'a, str>,
    ) -> Heartbeat {
        debug!("member `{}` send heartbeat to the `{}` group", member_id, group_id);

        let addr = AutoName::HostPort(coordinator.host(), coordinator.port());

        let request = KafkaRequest::heartbeat(
            self.next_correlation_id(),
            self.client_id(),
            group_id,
            group_generation_id,
            member_id,
        );

        self.send_request(addr, request)
            .and_then(|res| {
                if let KafkaResponse::Heartbeat(res) = res {
                    Ok(res.error_code)
                } else {
                    bail!(UnexpectedResponse(res.api_key()))
                }
            })
            .and_then(|error_code| {
                if error_code == KafkaCode::None as ErrorCode {
                    Ok(())
                } else {
                    bail!(KafkaError(error_code.into()))
                }
            })
            .static_boxed()
    }

    fn leave_group(&self, coordinator: &Broker, group_id: Cow<'a, str>, member_id: Cow<'a, str>) -> LeaveGroup {
        debug!("member `{}` leave the `{}` group", member_id, group_id);

        let addr = AutoName::HostPort(coordinator.host(), coordinator.port());

        let leaved_group_id: String = (*group_id).to_owned();

        let request = KafkaRequest::leave_group(self.next_correlation_id(), self.client_id(), group_id, member_id);

        self.send_request(addr, request)
            .and_then(|res| {
                if let KafkaResponse::LeaveGroup(res) = res {
                    Ok(res.error_code)
                } else {
                    bail!(UnexpectedResponse(res.api_key()))
                }
            })
            .and_then(|error_code| {
                if error_code == KafkaCode::None as ErrorCode {
                    Ok(leaved_group_id)
                } else {
                    bail!(KafkaError(error_code.into()))
                }
            })
            .static_boxed()
    }

    fn sync_group(
        &self,
        coordinator: &Broker,
        group_id: Cow<'a, str>,
        group_generation_id: GenerationId,
        member_id: Cow<'a, str>,
        group_assignment: Option<Vec<ConsumerGroupAssignment<'a>>>,
    ) -> SyncGroup {
        debug!(
            "sync the `{}` group as generation {} as member `{}`",
            group_id, group_generation_id, member_id
        );

        let addr = AutoName::HostPort(coordinator.host(), coordinator.port());

        let request = KafkaRequest::sync_group(
            self.next_correlation_id(),
            self.client_id(),
            group_id,
            group_generation_id,
            member_id,
            group_assignment.unwrap_or_default(),
        );

        self.send_request(addr, request)
            .and_then(|res| {
                if let KafkaResponse::SyncGroup(res) = res {
                    Ok(res)
                } else {
                    bail!(UnexpectedResponse(res.api_key()))
                }
            })
            .and_then(|res| {
                if res.error_code == KafkaCode::None as ErrorCode {
                    Ok(res.member_assignment)
                } else {
                    bail!(KafkaError(res.error_code.into()))
                }
            })
            .static_boxed()
    }
}

pub type FetchMetadata = StaticBoxFuture<Rc<Metadata>>;
pub type FetchApiVersions = StaticBoxFuture<UsableApiVersions>;
pub type LoadApiVersions = StaticBoxFuture<HashMap<BrokerRef, UsableApiVersions>>;

type TopicsByBroker<'a, T> = HashMap<((String, u16), ApiVersion), HashMap<Cow<'a, str>, Vec<(PartitionId, T)>>>;

impl State {
    pub fn next_correlation_id(&mut self) -> CorrelationId {
        self.correlation_id = self.correlation_id.wrapping_add(1);
        self.correlation_id - 1
    }

    pub fn metadata(&self) -> GetMetadata {
        match self.metadata_status {
            MetadataStatus::Loading(ref senders) => {
                let (sender, receiver) = oneshot::channel();
                senders.borrow_mut().push(sender);
                GetMetadata::Loading(receiver)
            }
            MetadataStatus::Loaded(ref metadata) => GetMetadata::Loaded(metadata.clone()),
        }
    }

    pub fn refresh_metadata(&mut self) {
        if let MetadataStatus::Loaded(_) = self.metadata_status {
            self.metadata_status = MetadataStatus::Loading(Default::default());
        }
    }

    pub fn update_metadata(&mut self, metadata: &Rc<Metadata>) {
        let status = mem::replace(&mut self.metadata_status, MetadataStatus::Loaded(metadata.clone()));

        if let MetadataStatus::Loading(senders) = status {
            for sender in senders.into_inner() {
                drop(sender.send(metadata.clone()));
            }
        }
    }
}

/// The future of loaded metadata
pub struct LoadMetadata<'a> {
    state: Loading,
    inner: Rc<Inner<'a>>,
}

pub enum Loading {
    Metadata(FetchMetadata),
    ApiVersions(Rc<Metadata>, LoadApiVersions),
    Finished(Rc<Metadata>),
}

impl<'a> LoadMetadata<'a>
where
    Self: 'static,
{
    fn new(inner: Rc<Inner<'a>>) -> LoadMetadata<'a> {
        let fetch_metadata = inner.fetch_all_metadata();

        (*inner.state).borrow_mut().refresh_metadata();

        LoadMetadata {
            state: Loading::Metadata(fetch_metadata),
            inner,
        }
    }
}

impl<'a> Future for LoadMetadata<'a>
where
    Self: 'static,
{
    type Item = Rc<Metadata>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let state;

            match self.state {
                Loading::Metadata(ref mut future) => match future.poll() {
                    Ok(Async::Ready(metadata)) => {
                        let inner = self.inner.clone();

                        if inner.config.api_version_request {
                            state = Loading::ApiVersions(metadata.clone(), inner.load_api_versions(&metadata));
                        } else {
                            let fallback_api_versions = inner.config.broker_version_fallback.api_versions();

                            let metadata = Rc::new(metadata.with_fallback_api_versions(fallback_api_versions));

                            trace!(
                                "use fallback API versions from {:?}, {:?}",
                                inner.config.broker_version_fallback,
                                fallback_api_versions
                            );

                            state = Loading::Finished(metadata);
                        }
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(err) => return Err(err),
                },
                Loading::ApiVersions(ref metadata, ref mut future) => match future.poll() {
                    Ok(Async::Ready(api_versions)) => {
                        let metadata = Rc::new(metadata.with_api_versions(&api_versions));

                        state = Loading::Finished(metadata);
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(err) => return Err(err),
                },
                Loading::Finished(ref metadata) => {
                    (*self.inner.state).borrow_mut().update_metadata(metadata);

                    return Ok(Async::Ready(metadata.clone()));
                }
            }

            self.state = state;
        }
    }
}

pub struct StaticBoxFuture<T = (), E = Error>(Box<Future<Item = T, Error = E> + 'static>)
where
    T: 'static,
    E: 'static;

impl<T, E> StaticBoxFuture<T, E> {
    pub fn new<F>(inner: F) -> Self
    where
        F: IntoFuture<Item = T, Error = E> + 'static,
        T: 'static,
        E: 'static,
    {
        StaticBoxFuture(Box::new(inner.into_future()))
    }

    pub fn ok(item: T) -> Self {
        StaticBoxFuture(Box::new(future::ok(item)))
    }

    pub fn err(err: E) -> Self {
        StaticBoxFuture(Box::new(future::err(err)))
    }
}

impl<T, E> From<ErrorKind> for StaticBoxFuture<T, E>
where
    E: From<ErrorKind>,
{
    fn from(err: ErrorKind) -> Self {
        Self::err(err.into())
    }
}

impl<T> From<Error> for StaticBoxFuture<T, Error> {
    fn from(err: Error) -> Self {
        Self::err(err)
    }
}

impl<T, E> Future for StaticBoxFuture<T, E> {
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

pub trait ToStaticBoxFuture<T, E> {
    fn static_boxed(self) -> StaticBoxFuture<T, E>;
}

impl<T, E, F> ToStaticBoxFuture<T, E> for F
where
    F: IntoFuture<Item = T, Error = E> + 'static,
    T: 'static,
    E: 'static + StdError,
{
    fn static_boxed(self) -> StaticBoxFuture<T, E> {
        StaticBoxFuture::new(self)
    }
}

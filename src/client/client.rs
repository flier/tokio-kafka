use std::rc::Rc;
use std::borrow::Cow;
use std::fmt::Debug;
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};
use std::net::{SocketAddr, ToSocketAddrs};
use std::collections::HashMap;
use std::iter::FromIterator;
use std::time::Duration;

use futures::{Async, Poll};
use futures::future::{self, Future};
use tokio_core::reactor::Handle;
use tokio_service::Service;

use errors::{Error, ErrorKind};
use protocol::{ApiKeys, ApiVersion, CorrelationId, ErrorCode, FetchOffset, KafkaCode, MessageSet,
               Offset, PartitionId, RequiredAcks, UsableApiVersions};
use network::{KafkaRequest, KafkaResponse, TopicPartition};
use client::{Broker, BrokerRef, ClientConfig, Cluster, KafkaService, Metadata, Metrics};

/// A retrieved offset for a particular partition in the context of an already known topic.
#[derive(Clone, Debug)]
pub struct PartitionOffset {
    pub partition: PartitionId,
    pub offset: Offset,
}

pub trait Client<'a>: 'static {
    fn produce_records(&self,
                       acks: RequiredAcks,
                       timeout: Duration,
                       TopicPartition<'a>,
                       records: Vec<Cow<'a, MessageSet>>)
                       -> ProduceRecords;

    fn fetch_offsets<S: AsRef<str>>(&self, topic_names: &[S], offset: FetchOffset) -> FetchOffsets;

    fn load_metadata(&mut self) -> LoadMetadata;
}

#[derive(Debug, Default)]
struct State {
    correlation_id: CorrelationId,
    metadata: Rc<Metadata>,
}

impl State {
    pub fn next_correlation_id(&mut self) -> CorrelationId {
        self.correlation_id = self.correlation_id.wrapping_add(1);
        self.correlation_id - 1
    }

    pub fn metadata(&self) -> Rc<Metadata> {
        self.metadata.clone()
    }

    pub fn update_metadata(&mut self, metadata: Rc<Metadata>) -> Rc<Metadata> {
        debug!("updating metadata, {:?}", metadata);

        self.metadata = metadata;
        self.metadata.clone()
    }

    pub fn update_api_versions(&mut self,
                               api_versions: HashMap<BrokerRef, UsableApiVersions>)
                               -> Rc<Metadata> {
        debug!("updating API versions, {:?}", api_versions);

        self.metadata = Rc::new(self.metadata.with_api_versions(api_versions));
        self.metadata.clone()
    }
}

pub struct KafkaClient<'a> {
    config: ClientConfig,
    handle: Handle,
    service: KafkaService<'a>,
    metrics: Option<Rc<Metrics>>,
    state: Rc<RefCell<State>>,
}

impl<'a> Deref for KafkaClient<'a> {
    type Target = ClientConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl<'a> DerefMut for KafkaClient<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl<'a> KafkaClient<'a>
    where Self: 'static
{
    pub fn from_hosts<I>(hosts: I, handle: Handle) -> KafkaClient<'a>
        where I: Iterator<Item = SocketAddr>
    {
        KafkaClient::from_config(ClientConfig::from_hosts(hosts), handle)
    }

    pub fn from_config(config: ClientConfig, handle: Handle) -> KafkaClient<'a> {
        let metrics = if config.metrics {
            Some(Rc::new(Metrics::new().expect("fail to register metrics")))
        } else {
            None
        };
        let service = KafkaService::new(handle.clone(),
                                        config.max_connection_idle(),
                                        config.request_timeout(),
                                        metrics.clone());

        KafkaClient {
            config: config,
            handle: handle,
            service: service,
            metrics: metrics,
            state: Rc::new(RefCell::new(State::default())),
        }
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    pub fn metrics(&self) -> Option<Rc<Metrics>> {
        self.metrics.clone()
    }

    pub fn metadata(&self) -> Rc<Metadata> {
        self.state.borrow().metadata()
    }

    fn next_correlation_id(&self) -> CorrelationId {
        self.state.borrow_mut().next_correlation_id()
    }

    fn client_id(&self) -> Option<Cow<'a, str>> {
        self.config.client_id.clone().map(Cow::from)
    }

    fn fetch_metadata<S>(&self, topic_names: &[S]) -> FetchMetadata
        where S: AsRef<str> + Debug
    {
        debug!("fetch metadata for toipcs: {:?}", topic_names);

        let responses = {
            let mut responses = Vec::new();

            for addr in &self.config.hosts {
                let request = KafkaRequest::fetch_metadata(0, // api_version
                                                           self.next_correlation_id(),
                                                           self.client_id(),
                                                           topic_names);

                let response = self.service
                    .call((*addr, request))
                    .and_then(|res| if let KafkaResponse::Metadata(res) = res {
                                  future::ok(Rc::new(Metadata::from(res)))
                              } else {
                                  future::err(ErrorKind::UnexpectedResponse(res.api_key()).into())
                              });

                responses.push(response);
            }

            responses
        };

        FetchMetadata::new(future::select_ok(responses).map(|(metadata, _)| metadata))
    }

    fn fetch_api_versions(&self, broker: &Broker) -> FetchApiVersions {
        debug!("fetch API versions for broker: {:?}", broker);

        let addr = broker
            .addr()
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap(); // TODO

        let request = KafkaRequest::fetch_api_versions(0, // api_version,
                                                       self.next_correlation_id(),
                                                       self.client_id());

        let response = self.service
            .call((addr, request))
            .and_then(|res| if let KafkaResponse::ApiVersions(res) = res {
                          future::ok(UsableApiVersions::new(res.api_versions))
                      } else {
                          future::err(ErrorKind::UnexpectedResponse(res.api_key()).into())
                      });

        FetchApiVersions::new(response)
    }

    fn load_api_verions(&self) -> LoadApiVersions {
        let metadata = self.state.borrow().metadata();
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
        let responses = future::join_all(responses).map(HashMap::from_iter);

        LoadApiVersions::new(StaticBoxFuture::new(responses), self.state.clone())
    }

    fn topics_by_broker<S>
        (&self,
         topic_names: &[S])
         -> HashMap<(SocketAddr, ApiVersion), HashMap<Cow<'a, str>, Vec<PartitionId>>>
        where S: AsRef<str>
    {
        let metadata = self.state.borrow().metadata();

        let mut topics = HashMap::new();

        for topic_name in topic_names.iter() {
            if let Some(partitions) = metadata.partitions_for_topic(topic_name.as_ref()) {
                for topic_partition in partitions {
                    if let Some(broker) = metadata.leader_for(&topic_partition) {
                        let addr = broker
                            .addr()
                            .to_socket_addrs()
                            .unwrap()
                            .next()
                            .unwrap(); // TODO
                        let api_version = broker
                            .api_versions()
                            .map_or(0, |api_versions| {
                                api_versions
                                    .find(ApiKeys::ListOffsets)
                                    .map(|api_version| api_version.max_version)
                                    .unwrap_or(0)
                            });
                        topics
                            .entry((addr, api_version))
                            .or_insert_with(HashMap::new)
                            .entry(topic_name.as_ref().to_owned().into())
                            .or_insert_with(Vec::new)
                            .push(topic_partition.partition);
                    }
                }
            }
        }

        topics
    }
}

impl<'a> Client<'a> for KafkaClient<'a>
    where Self: 'static
{
    fn produce_records(&self,
                       required_acks: RequiredAcks,
                       timeout: Duration,
                       tp: TopicPartition<'a>,
                       records: Vec<Cow<'a, MessageSet>>)
                       -> ProduceRecords {
        let request = KafkaRequest::produce_records(0, // api_version,
                                                    self.next_correlation_id(),
                                                    self.client_id(),
                                                    required_acks,
                                                    timeout,
                                                    &tp,
                                                    records);
        let addr = self.metadata()
            .leader_for(&tp)
            .map_or_else(|| *self.config.hosts.iter().next().unwrap(), |broker| {
                broker
                    .addr()
                    .to_socket_addrs()
                    .unwrap()
                    .next()
                    .unwrap()
            });
        let response = self.service
            .call((addr, request))
            .and_then(|res| if let KafkaResponse::Produce(res) = res {
                          let produce = res.topics
                              .iter()
                              .map(|topic| {
                    (topic.topic_name.to_owned(),
                     topic
                         .partitions
                         .iter()
                         .map(|partition| {
                                  (partition.partition, partition.error_code, partition.offset)
                              })
                         .collect())
                })
                              .collect();

                          future::ok(produce)
                      } else {
                          future::err(ErrorKind::UnexpectedResponse(res.api_key()).into())
                      });

        ProduceRecords::new(response)
    }

    fn fetch_offsets<S: AsRef<str>>(&self, topic_names: &[S], offset: FetchOffset) -> FetchOffsets {
        let topics = self.topics_by_broker(topic_names);

        let responses = {
            let mut responses = Vec::new();

            for ((addr, api_version), topics) in topics {
                let request = KafkaRequest::list_offsets(api_version,
                                                         self.next_correlation_id(),
                                                         self.client_id(),
                                                         topics,
                                                         offset);
                let response = self.service
                    .call((addr, request))
                    .and_then(|res| {
                        if let KafkaResponse::ListOffsets(res) = res {
                            let topics = res.topics
                                .iter()
                                .map(|topic| {
                                    let partitions = topic
                                        .partitions
                                        .iter()
                                        .flat_map(|partition| {
                                            if partition.error_code ==
                                               KafkaCode::None as ErrorCode {
                                                Ok(PartitionOffset {
                                                       partition: partition.partition,
                                                       offset: *partition
                                                                    .offsets
                                                                    .iter()
                                                                    .next()
                                                                    .unwrap(), // TODO
                                                   })
                                            } else {
                                                Err(ErrorKind::KafkaError(partition
                                                                              .error_code
                                                                              .into()))
                                            }
                                        })
                                        .collect();

                                    (topic.topic_name.clone(), partitions)
                                })
                                .collect::<Vec<(String, Vec<PartitionOffset>)>>();

                            Ok(topics)
                        } else {
                            bail!(ErrorKind::UnexpectedResponse(res.api_key()))
                        }
                    });

                responses.push(response);
            }

            responses
        };

        let offsets = future::join_all(responses).map(|responses| {
            responses
                .iter()
                .fold(HashMap::new(), |mut offsets, topics| {
                    for &(ref topic_name, ref partitions) in topics {
                        offsets
                            .entry(topic_name.clone())
                            .or_insert_with(Vec::new)
                            .extend(partitions.iter().cloned())
                    }
                    offsets
                })
        });

        FetchOffsets::new(offsets)
    }

    fn load_metadata(&mut self) -> LoadMetadata {
        debug!("loading metadata...");

        LoadMetadata::new(self.fetch_metadata::<&str>(&[]), self.state.clone())
    }
}

pub struct LoadMetadata {
    fetch_metadata: FetchMetadata,
    state: Rc<RefCell<State>>,
}

impl LoadMetadata {
    fn new(fetch_metadata: FetchMetadata, state: Rc<RefCell<State>>) -> Self {
        LoadMetadata {
            fetch_metadata: fetch_metadata,
            state: state,
        }
    }
}

impl Future for LoadMetadata
    where Self: 'static
{
    type Item = Rc<Metadata>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.fetch_metadata.poll() {
            Ok(Async::Ready(metadata)) => {
                self.state
                    .borrow_mut()
                    .update_metadata(metadata.clone());

                Ok(Async::Ready(metadata))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err),
        }
    }
}

pub struct LoadApiVersions {
    fetch_api_versions: StaticBoxFuture<HashMap<BrokerRef, UsableApiVersions>>,
    state: Rc<RefCell<State>>,
}

impl LoadApiVersions {
    fn new(fetch_api_versions: StaticBoxFuture<HashMap<BrokerRef, UsableApiVersions>>,
           state: Rc<RefCell<State>>)
           -> Self {
        LoadApiVersions {
            fetch_api_versions: fetch_api_versions,
            state: state,
        }
    }
}

impl Future for LoadApiVersions
    where Self: 'static
{
    type Item = Rc<Metadata>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.fetch_api_versions.poll() {
            Ok(Async::Ready(api_versions)) => {
                let metadata = self.state
                    .borrow_mut()
                    .update_api_versions(api_versions);

                Ok(Async::Ready(metadata))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err),
        }
    }
}

pub struct StaticBoxFuture<F = (), E = Error>(Box<Future<Item = F, Error = E> + 'static>);

impl<F, E> StaticBoxFuture<F, E> {
    pub fn new<T>(inner: T) -> Self
        where T: Future<Item = F, Error = E> + 'static
    {
        StaticBoxFuture(Box::new(inner))
    }
}

impl<F, E> Future for StaticBoxFuture<F, E> {
    type Item = F;
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

pub type SendRequest = StaticBoxFuture;
pub type ProduceRecords = StaticBoxFuture<HashMap<String, Vec<(PartitionId, ErrorCode, Offset)>>>;
pub type FetchOffsets = StaticBoxFuture<HashMap<String, Vec<PartitionOffset>>>;
pub type FetchMetadata = StaticBoxFuture<Rc<Metadata>>;
pub type FetchApiVersions = StaticBoxFuture<UsableApiVersions>;

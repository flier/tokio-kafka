use std::rc::Rc;
use std::cell::RefCell;
use std::borrow::{Borrow, Cow};
use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;

use time;

use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream, future};
use tokio_core::reactor::{Handle, Timeout};
use tokio_retry::Retry;

use errors::Error;
use protocol::{ApiKeys, PartitionId, ToMilliseconds};
use network::TopicPartition;
use client::{Cluster, KafkaClient, Metadata, StaticBoxFuture};
use producer::{Accumulator, Interceptors, PartitionRecord, Partitioner, ProducerBuilder,
               ProducerConfig, ProducerInterceptor, ProducerInterceptors, ProducerRecord,
               PushRecord, RecordAccumulator, RecordMetadata, Sender, Serializer, TopicRecord};

pub trait Producer<'a> {
    type Key: Hash;
    type Value;
    type Topic: Sink<SinkItem = TopicRecord<Self::Key, Self::Value>, SinkError = Error>;
    type Partition: Sink<SinkItem = PartitionRecord<Self::Key, Self::Value>, SinkError = Error>;

    /// Send the given record asynchronously and
    /// return a future which will eventually contain the response information.
    fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord;

    /// Flush any accumulated records from the producer.
    fn flush(&mut self) -> Flush;

    fn topic(&self, topic_name: &str) -> Self::Topic;

    fn partition(&self, topic_name: &str, partition_id: PartitionId) -> Self::Partition;
}

pub type SendRecord = StaticBoxFuture<RecordMetadata>;
pub type Flush = StaticBoxFuture;

#[derive(Clone)]
pub struct KafkaProducer<'a, K, V, P>
    where K: Serializer,
          K::Item: Hash,
          V: Serializer
{
    inner: Rc<Inner<'a, K, V, P>>,
}

struct Inner<'a, K, V, P>
    where K: Serializer,
          K::Item: Hash,
          V: Serializer
{
    client: Rc<KafkaClient<'a>>,
    config: ProducerConfig,
    accumulator: RecordAccumulator<'a>,
    key_serializer: K,
    value_serializer: V,
    partitioner: P,
    interceptors: Interceptors<K::Item, V::Item>,
}

impl<'a, K, V, P> KafkaProducer<'a, K, V, P>
    where K: Serializer,
          K::Item: Hash,
          V: Serializer,
          Self: 'static
{
    pub fn new(client: KafkaClient<'a>,
               config: ProducerConfig,
               key_serializer: K,
               value_serializer: V,
               partitioner: P,
               interceptors: Interceptors<K::Item, V::Item>)
               -> Self {
        let accumulator =
            RecordAccumulator::new(config.batch_size, config.compression, config.linger());

        KafkaProducer {
            inner: Rc::new(Inner {
                               client: Rc::new(client),
                               config: config,
                               accumulator: accumulator,
                               key_serializer: key_serializer,
                               value_serializer: value_serializer,
                               partitioner: partitioner,
                               interceptors: interceptors,
                           }),
        }
    }

    pub fn from_client(client: KafkaClient<'a>) -> ProducerBuilder<'a, K, V, P>
        where K: Serializer,
              V: Serializer
    {
        ProducerBuilder::from_client(client)
    }

    pub fn from_hosts<I>(hosts: I, handle: Handle) -> ProducerBuilder<'a, K, V, P>
        where I: Iterator<Item = SocketAddr>,
              K: Serializer,
              V: Serializer
    {
        ProducerBuilder::from_config(ProducerConfig::from_hosts(hosts), handle)
    }
}

impl<'a, K, V, P> Producer<'a> for KafkaProducer<'a, K, V, P>
    where K: Serializer,
          K::Item: Debug + Hash,
          V: Serializer,
          V::Item: Debug,
          P: Partitioner,
          Self: 'static
{
    type Key = K::Item;
    type Value = V::Item;
    type Topic = Topic<'a, K, V, P>;
    type Partition = Partition<'a, K, V, P>;

    fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord {
        let inner = self.inner.clone();

        let future = self.inner
            .client
            .metadata()
            .and_then(move |metadata| {
                let push_record = inner.push_record(metadata, record);

                if push_record.is_full() {
                    let flush = inner
                        .flush_batches(false)
                        .map_err(|err| {
                                     warn!("fail to flush full batch, {}", err);
                                 });

                    inner.client.handle().spawn(flush);
                }

                if push_record.new_batch() {
                    let timeout = Timeout::new(inner.config.linger(), inner.client.handle());

                    match timeout {
                        Ok(timeout) => {
                            let future = {
                                let inner = inner.clone();

                                timeout
                                    .map_err(Error::from)
                                    .and_then(move |_| inner.flush_batches(false))
                                    .map(|_| ())
                                    .map_err(|_| ())
                            };

                            inner.clone().client.handle().spawn(future);
                        }
                        Err(err) => {
                            warn!("fail to create timeout, {}", err);
                        }
                    }
                }

                push_record
            });

        SendRecord::new(future)
    }

    fn flush(&mut self) -> Flush {
        self.inner.flush_batches(true)
    }

    fn topic(&self, topic_name: &str) -> Self::Topic {
        Topic {
            producer: KafkaProducer { inner: self.inner.clone() },
            topic_name: topic_name.to_owned().into(),
            flushing: None,
        }
    }

    fn partition(&self, topic_name: &str, partition_id: PartitionId) -> Self::Partition {
        Partition {
            producer: KafkaProducer { inner: self.inner.clone() },
            topic_name: topic_name.to_owned().into(),
            partition_id: partition_id,
            flushing: None,
        }
    }
}

impl<'a, K, V, P> Inner<'a, K, V, P>
    where K: Serializer,
          K::Item: Debug + Hash,
          V: Serializer,
          V::Item: Debug,
          P: Partitioner,
          Self: 'static
{
    fn push_record(&self,
                   metadata: Rc<Metadata>,
                   mut record: ProducerRecord<K::Item, V::Item>)
                   -> PushRecord {
        trace!("sending record {:?}", record);

        if let Some(ref interceptors) = self.interceptors {
            let interceptors: &RefCell<ProducerInterceptors<K::Item, V::Item>> = interceptors
                .borrow();

            record = match interceptors.borrow().send(record) {
                Ok(record) => record,
                Err(err) => return PushRecord::new(future::err(err), false, false),
            }
        }

        let ProducerRecord {
            topic_name,
            partition,
            key,
            value,
            timestamp,
        } = record;

        let partition = self.partitioner
            .partition(&topic_name,
                       partition,
                       key.as_ref(),
                       value.as_ref(),
                       metadata.clone())
            .unwrap_or_default();

        let key = key.and_then(|key| {
                                   self.key_serializer
                                       .serialize(&topic_name, key)
                                       .map_err(|err| warn!("fail to serialize key, {}", err))
                                       .ok()
                               });

        let value =
            value.and_then(|value| {
                               self.value_serializer
                                   .serialize(&topic_name, value)
                                   .map_err(|err| warn!("fail to serialize value, {}", err))
                                   .ok()
                           });

        let tp = TopicPartition {
            topic_name: topic_name.into(),
            partition: partition,
        };

        let timestamp =
            timestamp.unwrap_or_else(|| time::now_utc().to_timespec().as_millis() as i64);

        let api_version = metadata
            .leader_for(&tp)
            .and_then(|broker| broker.api_versions())
            .and_then(|api_versions| api_versions.find(ApiKeys::Produce))
            .map_or(0, |api_version| api_version.max_version);

        trace!("use API version {} for {:?}", api_version, tp);

        self.accumulator
            .push_record(tp, timestamp, key, value, api_version)
    }

    /// Flush full or expired batches
    fn flush_batches(&self, force: bool) -> Flush {
        let client = self.client.clone();
        let interceptor = self.interceptors.clone();
        let handle = self.client.handle().clone();
        let acks = self.config.acks;
        let ack_timeout = self.config.ack_timeout();
        let retry_strategy = self.config.retry_strategy();

        Flush::new(self.accumulator
                       .batches(force)
                       .for_each(move |(tp, batch)| {
            let sender = Sender::new(client.clone(),
                                     interceptor.clone(),
                                     acks,
                                     ack_timeout,
                                     tp,
                                     batch);

            match sender {
                Ok(sender) => {
                    StaticBoxFuture::new(Retry::spawn(handle.clone(),
                                                      retry_strategy.clone(),
                                                      move || sender.send_batch())
                                                 .map_err(Error::from))
                }
                Err(err) => {
                    warn!("fail to create sender, {}", err);

                    StaticBoxFuture::new(future::err(err))
                }
            }
        }))
    }
}

pub struct Topic<'a, K, V, P>
    where K: Serializer,
          K::Item: Debug + Hash,
          V: Serializer,
          V::Item: Debug,
          P: Partitioner
{
    producer: KafkaProducer<'a, K, V, P>,
    topic_name: Cow<'a, str>,
    flushing: Option<Flush>,
}

impl<'a, K, V, P> Sink for Topic<'a, K, V, P>
    where K: Serializer,
          K::Item: Debug + Hash,
          V: Serializer,
          V::Item: Debug,
          P: Partitioner,
          Self: 'static
{
    type SinkItem = TopicRecord<K::Item, V::Item>;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let record = ProducerRecord::from_topic_record(self.topic_name.as_ref(), item);
        drop(Producer::send(&mut self.producer, record));

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let mut flushing = if let Some(flushing) = self.flushing.take() {
            flushing
        } else {
            self.producer.flush()
        };

        let polling = flushing.poll();

        if let Ok(Async::NotReady) = polling {
            self.flushing = Some(flushing);
        }

        polling
    }
}

pub struct Partition<'a, K, V, P>
    where K: Serializer,
          K::Item: Debug + Hash,
          V: Serializer,
          V::Item: Debug,
          P: Partitioner
{
    producer: KafkaProducer<'a, K, V, P>,
    topic_name: Cow<'a, str>,
    partition_id: PartitionId,
    flushing: Option<Flush>,
}

impl<'a, K, V, P> Sink for Partition<'a, K, V, P>
    where K: Serializer,
          K::Item: Debug + Hash,
          V: Serializer,
          V::Item: Debug,
          P: Partitioner,
          Self: 'static
{
    type SinkItem = PartitionRecord<K::Item, V::Item>;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let record = ProducerRecord::from_partition_record(self.topic_name.as_ref(),
                                                           self.partition_id,
                                                           item);
        drop(Producer::send(&mut self.producer, record));

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let mut flushing = if let Some(flushing) = self.flushing.take() {
            flushing
        } else {
            self.producer.flush()
        };

        let polling = flushing.poll();

        if let Ok(Async::NotReady) = polling {
            self.flushing = Some(flushing);
        }

        polling
    }
}

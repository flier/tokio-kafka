use std::rc::Rc;
use std::cell::RefCell;
use std::borrow::{Borrow, Cow};
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use std::net::SocketAddr;

use time;

use futures::{Future, Poll, Stream};
use tokio_core::reactor::Handle;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use errors::Error;
use protocol::{ApiKeys, MessageSet, RequiredAcks};
use network::TopicPartition;
use client::{Client, Cluster, KafkaClient, Metadata, StaticBoxFuture, ToMilliseconds};
use producer::{Accumulator, Partitioner, ProducerBatch, ProducerBuilder, ProducerConfig,
               ProducerRecord, RecordAccumulator, RecordMetadata, Serializer, Thunk};

pub trait Producer<'a> {
    type Key: Hash;
    type Value;

    /// Send the given record asynchronously and
    /// return a future which will eventually contain the response information.
    fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord;

    /// Flush any accumulated records from the producer.
    fn flush(&mut self, force: bool) -> Flush;
}

pub type SendRecord = StaticBoxFuture<RecordMetadata>;
pub type Flush = StaticBoxFuture;

pub struct KafkaProducer<'a, K, V, P> {
    client: Rc<RefCell<KafkaClient<'a>>>,
    config: ProducerConfig,
    accumulators: RecordAccumulator<'a>,
    key_serializer: K,
    value_serializer: V,
    partitioner: P,
}

impl<'a, K, V, P> KafkaProducer<'a, K, V, P>
    where Self: 'static
{
    pub fn new(client: KafkaClient<'a>,
               config: ProducerConfig,
               key_serializer: K,
               value_serializer: V,
               partitioner: P)
               -> Self {
        let accumulators = RecordAccumulator::new(config.batch_size,
                                                  config.compression,
                                                  Duration::from_millis(config.linger),
                                                  Duration::from_millis(config.retry_backoff));

        KafkaProducer {
            client: Rc::new(RefCell::new(client)),
            config: config,
            accumulators: accumulators,
            key_serializer: key_serializer,
            value_serializer: value_serializer,
            partitioner: partitioner,
        }
    }

    pub fn client(&self) -> Rc<RefCell<KafkaClient<'a>>> {
        self.client.clone()
    }

    pub fn from_client(client: KafkaClient<'a>) -> ProducerBuilder<'a, K, V, P> {
        ProducerBuilder::from_client(client)
    }

    pub fn from_hosts<I>(hosts: I, handle: Handle) -> ProducerBuilder<'a, K, V, P>
        where I: Iterator<Item = SocketAddr>
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

    fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord {
        trace!("sending record {:?}", record);

        let ProducerRecord {
            topic_name,
            partition,
            key,
            value,
            timestamp,
        } = record;

        let client: &RefCell<KafkaClient> = self.client.borrow();
        let cluster: Rc<Metadata> = client.borrow().metadata();

        let partition = self.partitioner
            .partition(&topic_name,
                       partition,
                       key.as_ref(),
                       value.as_ref(),
                       cluster.clone())
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

        let api_version = cluster
            .leader_for(&tp)
            .and_then(|broker| broker.api_versions())
            .and_then(|api_versions| api_versions.find(ApiKeys::Produce))
            .map_or(0, |api_version| api_version.max_version);

        SendRecord::new(self.accumulators
                            .push_record(tp, timestamp, key, value, api_version))
    }

    fn flush(&mut self, force: bool) -> Flush {
        if force {
            trace!("force to flush batches");

            self.accumulators.flush();
        }

        let client = self.client.clone();
        let handle = {
            let client: &RefCell<KafkaClient> = self.client.borrow();

            client.borrow().handle().clone()
        };
        let acks = self.config.acks;
        let ack_timeout = self.config.ack_timeout();
        let retry_strategy = ExponentialBackoff::from_millis(self.config.retry_backoff)
            .map(jitter)
            .take(self.config.retries)
            .collect::<Vec<Duration>>();

        Flush::new(self.accumulators
                       .batches()
                       .for_each(move |(tp, batch)| {
            let sender = Sender::new(client.clone(), acks, ack_timeout, tp, batch);

            Retry::spawn(handle.clone(),
                         retry_strategy.clone(),
                         move || sender.send_batch())
                    .map_err(Error::from)
        }))
    }
}

struct Sender<'a> {
    inner: Rc<RefCell<SenderInner<'a>>>,
}

pub struct SenderInner<'a> {
    client: Rc<RefCell<KafkaClient<'a>>>,
    acks: RequiredAcks,
    ack_timeout: Duration,
    tp: TopicPartition<'a>,
    thunks: Rc<RefCell<Option<Vec<Thunk>>>>,
    message_set: MessageSet,
}

impl<'a> Sender<'a>
    where Self: 'static
{
    pub fn new(client: Rc<RefCell<KafkaClient<'a>>>,
               acks: RequiredAcks,
               ack_timeout: Duration,
               tp: TopicPartition<'a>,
               batch: ProducerBatch)
               -> Sender<'a> {
        let (thunks, message_set) = batch.build();
        let inner = SenderInner {
            client: client,
            acks: acks,
            ack_timeout: ack_timeout,
            tp: tp,
            thunks: Rc::new(RefCell::new(Some(thunks))),
            message_set: message_set,
        };
        Sender { inner: Rc::new(RefCell::new(inner)) }
    }

    pub fn send_batch(&self) -> SendBatch<'a> {
        let inner = self.inner.clone();

        let send_batch = {
            let inner: &RefCell<SenderInner> = inner.borrow();
            let inner = inner.borrow();
            inner.send_batch()
        };

        SendBatch::new(inner, StaticBoxFuture::new(send_batch))
    }
}

impl<'a> SenderInner<'a>
    where Self: 'static
{
    pub fn send_batch(&self) -> StaticBoxFuture {
        trace!("sending batch to {:?}: {:?}", self.tp, self.message_set);

        let topic_name: String = String::from(self.tp.topic_name.borrow());
        let partition = self.tp.partition;
        let acks = self.acks;
        let ack_timeout = self.ack_timeout;
        let message_set = Cow::Owned(self.message_set.clone());
        let thunks = self.thunks.clone();

        let client: &RefCell<KafkaClient> = self.client.borrow();

        let send_batch = client
            .borrow()
            .produce_records(acks,
                             ack_timeout,
                             vec![(TopicPartition {
                                       topic_name: topic_name.clone().into(),
                                       partition: partition,
                                   },
                                   message_set)])
            .map(move |responses| {
                responses
                    .get(&topic_name)
                    .map(|partitions| {
                        partitions
                            .iter()
                            .find(|&&(partition_id, _, _)| partition_id == partition)
                            .map(|&(_, error_code, offset)| {
                                let thunks: &RefCell<Option<Vec<Thunk>>> = thunks.borrow();

                                if let Some(thunks) = thunks.borrow_mut().take() {
                                    for thunk in thunks {
                                        match thunk.done(&topic_name,
                                                         partition,
                                                         offset,
                                                         error_code.into()) {
                                            Ok(()) => {}
                                            Err(metadata) => {
                                                warn!("fail to send record metadata, {:?}",
                                                      metadata)
                                            }
                                        }
                                    }
                                }
                            });
                    });
            });

        StaticBoxFuture::new(send_batch)
    }
}

pub struct SendBatch<'a> {
    inner: Rc<RefCell<SenderInner<'a>>>,
    future: StaticBoxFuture,
}

impl<'a> SendBatch<'a> {
    pub fn new(inner: Rc<RefCell<SenderInner<'a>>>, future: StaticBoxFuture) -> SendBatch<'a> {
        SendBatch {
            inner: inner,
            future: future,
        }
    }
}

impl<'a> Future for SendBatch<'a> {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.future.poll()
    }
}

#[cfg(test)]
pub mod mock {
    use std::hash::Hash;

    use futures::future;

    use producer::{Flush, Producer, ProducerRecord, RecordMetadata, SendRecord};

    #[derive(Debug, Default)]
    pub struct MockProducer<K, V>
        where K: Hash
    {
        pub records: Vec<(Option<K>, Option<V>)>,
    }

    impl<'a, K, V> Producer<'a> for MockProducer<K, V>
        where K: Hash + Clone,
              V: Clone
    {
        type Key = K;
        type Value = V;

        fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord {
            self.records.push((record.key, record.value));

            SendRecord::new(future::ok(RecordMetadata::default()))
        }

        fn flush(&mut self, _force: bool) -> Flush {
            Flush::new(future::ok(()))
        }
    }
}

use std::borrow::Cow;
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;

use futures::{Future, Stream};

use client::{Cluster, KafkaClient, StaticBoxFuture, ToStaticBoxFuture};
use consumer::{ConsumerConfig, ConsumerCoordinator, Fetcher, SubscribedTopics, Subscriptions};
use errors::{Error, ErrorKind};
use protocol::{MessageTimestamp, Offset, PartitionId};
use serialization::Deserializer;

/// A trait for consuming records from a Kafka cluster.
pub trait Consumer<'a> {
    /// The type of key
    type Key: Hash;
    /// The type of value
    type Value;
    /// The type of `Stream` to receive records from topics
    type Topics: Stream<Item = ConsumerRecord<'a, Self::Key, Self::Value>, Error = Error>;

    /// Subscribe to the given list of topics to get dynamically assigned partitions.
    fn subscribe<S>(&mut self, topic_names: &[S]) -> Subscribe<Self::Topics>
    where
        S: AsRef<str> + Hash + Eq;
}

/// A key/value pair to be received from Kafka.
///
/// This also consists of a topic name and a partition number from which the record is being
/// received, an offset that points to the record in a Kafka partition, and a timestamp as marked
/// by the corresponding `ConsumerRecord`.
pub struct ConsumerRecord<'a, K, V> {
    /// The topic this record is received from
    pub topic_name: Cow<'a, str>,
    /// The partition from which this record is received
    pub partition_id: PartitionId,
    /// The position of this record in the corresponding Kafka partition.
    pub offset: Offset,
    /// The key (or None if no key is specified)
    pub key: Option<K>,
    /// The value
    pub value: Option<V>,
    /// The timestamp of this record
    pub timestamp: Option<MessageTimestamp>,
}

pub type Subscribe<T> = StaticBoxFuture<T>;

/// A Kafka consumer that consumes records from a Kafka cluster.
pub struct KafkaConsumer<'a, K, V> {
    inner: Rc<Inner<'a, K, V>>,
}

struct Inner<'a, K, V> {
    client: KafkaClient<'a>,
    config: ConsumerConfig,
    key_deserializer: K,
    value_deserializer: V,
}

impl<'a, K, V> KafkaConsumer<'a, K, V> {
    pub fn new(
        client: KafkaClient<'a>,
        config: ConsumerConfig,
        key_deserializer: K,
        value_deserializer: V,
    ) -> Self {
        KafkaConsumer {
            inner: Rc::new(Inner {
                client: client,
                config: config,
                key_deserializer: key_deserializer,
                value_deserializer: value_deserializer,
            }),
        }
    }
}

impl<'a, K, V> KafkaConsumer<'a, K, V>
where
    K: Deserializer + Clone,
{
    pub fn key_deserializer(&self) -> K {
        self.inner.key_deserializer.clone()
    }
}

impl<'a, K, V> KafkaConsumer<'a, K, V>
where
    V: Deserializer + Clone,
{
    pub fn value_deserializer(&self) -> V {
        self.inner.value_deserializer.clone()
    }
}

impl<'a, K, V> Consumer<'a> for KafkaConsumer<'a, K, V>
where
    K: Deserializer + Clone,
    K::Item: Hash,
    V: Deserializer + Clone,
    Self: 'static,
{
    type Key = K::Item;
    type Value = V::Item;
    type Topics = SubscribedTopics<'a, K, V>;

    fn subscribe<S>(&mut self, topic_names: &[S]) -> Subscribe<Self::Topics>
    where
        S: AsRef<str> + Hash + Eq,
    {
        let topic_names: Vec<String> = topic_names.iter().map(|s| s.as_ref().to_owned()).collect();
        let inner = self.inner.clone();
        let default_reset_strategy = self.inner.config.auto_offset_reset;
        let group_id = self.inner.config.group_id.clone();
        let session_timeout = self.inner.config.session_timeout();
        let rebalance_timeout = self.inner.config.rebalance_timeout();
        let heartbeat_interval = self.inner.config.heartbeat_interval();
        let fetch_min_bytes = self.inner.config.fetch_min_bytes;
        let fetch_max_bytes = self.inner.config.fetch_max_bytes;
        let fetch_max_wait = self.inner.config.fetch_max_wait();
        let partition_fetch_bytes = self.inner.config.partition_fetch_bytes;
        let assignors = self.inner
            .config
            .assignment_strategy
            .iter()
            .map(|strategy| strategy.assignor())
            .collect();
        let timer = self.inner.client.timer().clone();

        self.inner
            .client
            .metadata()
            .and_then(move |metadata| {
                let topics = metadata.topics();
                let not_found: Vec<String> = topic_names
                    .iter()
                    .filter(|topic_name| !topics.contains_key(topic_name.as_str()))
                    .cloned()
                    .collect();

                if not_found.is_empty() {
                    let subscriptions = Rc::new(RefCell::new(Subscriptions::with_topics(
                        topic_names.iter(),
                        default_reset_strategy,
                    )));
                    let coordinator = ConsumerCoordinator::new(
                        inner.client.clone(),
                        group_id,
                        subscriptions.clone(),
                        session_timeout,
                        rebalance_timeout,
                        heartbeat_interval,
                        None,
                        assignors,
                        timer,
                    );

                    let fetcher = Fetcher::new(
                        inner.client.clone(),
                        subscriptions.clone(),
                        fetch_min_bytes,
                        fetch_max_bytes,
                        fetch_max_wait,
                        partition_fetch_bytes,
                    );

                    SubscribedTopics::new(
                        KafkaConsumer { inner: inner },
                        subscriptions,
                        coordinator,
                        fetcher,
                    )
                } else {
                    bail!(ErrorKind::TopicNotFound(not_found.join(",")))
                }
            })
            .static_boxed()
    }
}

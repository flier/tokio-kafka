use std::borrow::Cow;
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use std::ops::Deref;

use futures::{Future, Stream};
use tokio_core::reactor::Handle;

use client::{Client, Cluster, KafkaClient, StaticBoxFuture, ToStaticBoxFuture};
use consumer::{ConsumerBuilder, ConsumerConfig, ConsumerCoordinator, Fetcher, SubscribedTopics, Subscriptions};
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

    /// Subscribe to the given list of topics to get dynamically assigned
    /// partitions.
    fn subscribe<I, S>(&mut self, topic_names: I) -> Subscribe<Self::Topics>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>;
}

/// A key/value pair to be received from Kafka.
///
/// This also consists of a topic name and a partition number from which the record is being
/// received, an offset that points to the record in a Kafka partition, and a timestamp as marked
/// by the corresponding `ConsumerRecord`.
#[derive(Clone, Debug)]
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
#[derive(Clone)]
pub struct KafkaConsumer<'a, K, V> {
    inner: Rc<Inner<'a, K, V>>,
}

struct Inner<'a, K, V> {
    client: KafkaClient<'a>,
    config: ConsumerConfig,
    key_deserializer: K,
    value_deserializer: V,
}

impl<'a, K, V> Deref for KafkaConsumer<'a, K, V> {
    type Target = KafkaClient<'a>;

    fn deref(&self) -> &Self::Target {
        &self.inner.client
    }
}

impl<'a, K, V> KafkaConsumer<'a, K, V> {
    /// Construct a `KafkaConsumer`
    pub fn new(client: KafkaClient<'a>, config: ConsumerConfig, key_deserializer: K, value_deserializer: V) -> Self {
        KafkaConsumer {
            inner: Rc::new(Inner {
                client,
                config,
                key_deserializer,
                value_deserializer,
            }),
        }
    }

    /// Construct a `ConsumerBuilder` from ConsumerConfig
    pub fn with_config(config: ConsumerConfig, handle: Handle) -> ConsumerBuilder<'a, K, V> {
        ConsumerBuilder::with_config(config, handle)
    }

    /// Construct a `ConsumerBuilder` from bootstrap servers of the Kafka
    /// cluster
    pub fn with_bootstrap_servers<I>(hosts: I, handle: Handle) -> ConsumerBuilder<'a, K, V>
    where
        I: IntoIterator<Item = String>,
    {
        ConsumerBuilder::with_bootstrap_servers(hosts, handle)
    }

    pub fn config(&self) -> &ConsumerConfig {
        &self.inner.config
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

    fn subscribe<I, S>(&mut self, topic_names: I) -> Subscribe<Self::Topics>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let topic_names: Vec<String> = topic_names.into_iter().map(|s| s.into()).collect();
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
        let auto_commit_interval = self.inner.config.auto_commit_interval();
        let assignors = self.inner
            .config
            .assignment_strategy
            .iter()
            .flat_map(|strategy| strategy.assignor())
            .collect();
        let timer = self.inner.client.timer().clone();

        self.inner
            .client
            .metadata()
            .and_then(move |metadata| {
                let topics = metadata.topics();

                if let Some(not_found) = topic_names
                    .iter()
                    .find(|topic_name| !topics.contains_key(topic_name.as_str()))
                {
                    bail!(ErrorKind::TopicNotFound(not_found.clone()))
                }

                let subscriptions = Rc::new(RefCell::new(Subscriptions::with_topics(
                    topic_names,
                    default_reset_strategy,
                )));

                let coordinator = group_id.map(|group_id| {
                    ConsumerCoordinator::new(
                        inner.client.clone(),
                        group_id,
                        subscriptions.clone(),
                        session_timeout,
                        rebalance_timeout,
                        heartbeat_interval,
                        None,
                        auto_commit_interval,
                        assignors,
                        timer.clone(),
                    )
                });

                let fetcher = Rc::new(Fetcher::new(
                    inner.client.clone(),
                    subscriptions.clone(),
                    fetch_min_bytes,
                    fetch_max_bytes,
                    fetch_max_wait,
                    partition_fetch_bytes,
                ));

                SubscribedTopics::new(KafkaConsumer { inner }, subscriptions, coordinator, fetcher, timer)
            })
            .static_boxed()
    }
}

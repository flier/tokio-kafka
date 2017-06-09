use std::rc::Rc;
use std::cell::RefCell;
use std::hash::Hash;

use futures::{Async, Future, Poll, Stream};

use errors::{Error, ErrorKind};
use serialization::Deserializer;
use client::{Cluster, KafkaClient, StaticBoxFuture, ToStaticBoxFuture, TopicRecord};
use consumer::{ConsumerConfig, ConsumerCoordinator, Coordinator, Fetcher, Subscriptions};

/// A trait for consuming records from a Kafka cluster.
pub trait Consumer {
    /// The type of key
    type Key: Hash;
    /// The type of value
    type Value;
    /// The type of `Stream` to receive records from topics
    type Topics: Stream<Item = TopicRecord<Self::Key, Self::Value>, Error = Error>;

    fn subscribe<S>(&mut self, topic_names: &[S]) -> Subscriber<Self::Topics>
        where S: AsRef<str> + Hash + Eq;
}

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

impl<'a, K, V> KafkaConsumer<'a, K, V>
    where K: Deserializer,
          V: Deserializer,
          Self: 'static
{
    pub fn new(client: KafkaClient<'a>,
               config: ConsumerConfig,
               key_deserializer: K,
               value_deserializer: V)
               -> Self {
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

impl<'a, K, V> Consumer for KafkaConsumer<'a, K, V>
    where K: Deserializer,
          K::Item: Hash,
          V: Deserializer,
          Self: 'static
{
    type Key = K::Item;
    type Value = V::Item;
    type Topics = ConsumerTopics<'a, K, V>;

    fn subscribe<S>(&mut self, topic_names: &[S]) -> Subscriber<Self::Topics>
        where S: AsRef<str> + Hash + Eq
    {
        let topic_names: Vec<String> = topic_names.iter().map(|s| s.as_ref().to_owned()).collect();
        let inner = self.inner.clone();
        let default_reset_strategy = self.inner.config.auto_offset_reset;
        let group_id = self.inner.config.group_id.clone();
        let session_timeout = self.inner.config.session_timeout();
        let rebalance_timeout = self.inner.config.rebalance_timeout();
        let heartbeat_interval = self.inner.config.heartbeat_interval();
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
                    let subscriptions =
                        Rc::new(RefCell::new(Subscriptions::with_topics(topic_names.iter(),
                                                                        default_reset_strategy)));
                    let coordinator = ConsumerCoordinator::new(inner.client.clone(),
                                                               group_id,
                                                               subscriptions.clone(),
                                                               session_timeout,
                                                               rebalance_timeout,
                                                               heartbeat_interval,
                                                               assignors,
                                                               timer);
                    let fetcher = Fetcher::new(inner.client.clone(), subscriptions);

                    Ok(ConsumerTopics {
                           consumer: KafkaConsumer { inner: inner },
                           coordinator: coordinator,
                           fetcher: fetcher,
                       })
                } else {
                    bail!(ErrorKind::TopicNotFound(not_found.join(",")))
                }
            })
            .static_boxed()
    }
}

pub type Subscriber<T> = StaticBoxFuture<T>;

pub struct ConsumerTopics<'a, K, V> {
    consumer: KafkaConsumer<'a, K, V>,
    coordinator: ConsumerCoordinator<'a>,
    fetcher: Fetcher<'a>,
}

impl<'a, K, V> ConsumerTopics<'a, K, V>
    where K: Deserializer,
          K::Item: Hash,
          V: Deserializer,
          Self: 'static
{
    pub fn commit(&mut self) -> Commit {
        Commit::ok(())
    }

    /// Unsubscribe from topics currently subscribed with `Consumer::subscribe`
    pub fn unsubscribe(mut self) -> Unsubscribe {
        self.coordinator.leave_group().static_boxed()
    }
}

pub type Commit = StaticBoxFuture;
pub type Unsubscribe = StaticBoxFuture;

impl<'a, K, V> Stream for ConsumerTopics<'a, K, V>
    where K: Deserializer,
          K::Item: Hash,
          V: Deserializer
{
    type Item = TopicRecord<K::Item, V::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::NotReady)
    }
}

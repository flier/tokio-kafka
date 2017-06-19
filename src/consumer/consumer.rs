use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;

use futures::{Async, Future, Poll, Stream};

use client::{Cluster, KafkaClient, StaticBoxFuture, ToStaticBoxFuture, TopicRecord};
use consumer::{CommitOffset, ConsumerConfig, ConsumerCoordinator, Coordinator, Fetcher,
               LeaveGroup, SeekTo, Subscriptions};
use errors::{Error, ErrorKind, Result};
use network::{OffsetAndMetadata, TopicPartition};
use protocol::Offset;
use serialization::Deserializer;

/// A trait for consuming records from a Kafka cluster.
pub trait Consumer {
    /// The type of key
    type Key: Hash;
    /// The type of value
    type Value;
    /// The type of `Stream` to receive records from topics
    type Topics: Stream<Item = TopicRecord<Self::Key, Self::Value>, Error = Error>;

    fn subscribe<S>(&mut self, topic_names: &[S]) -> Subscribe<Self::Topics>
    where
        S: AsRef<str> + Hash + Eq;
}

pub type Subscribe<T> = StaticBoxFuture<T>;

/// A trait for to the subscribed list of topics.
pub trait Subscribed<'a> {
    /// Get the set of partitions currently assigned to this consumer.
    fn assigment(&self) -> Vec<TopicPartition<'a>>;

    /// Get the current subscription.
    fn subscription(&self) -> Vec<String>;

    /// Unsubscribe from topics currently subscribed with `Consumer::subscribe`
    fn unsubscribe(self) -> Unsubscribe;

    /// Commit offsets returned on the last record for all the subscribed list of topics and
    /// partitions.
    fn commit(&mut self) -> Commit;

    /// Overrides the fetch offsets that the consumer will use on the next record
    fn seek(&self, partition: &TopicPartition<'a>, pos: SeekTo) -> Result<()>;

    /// Seek to the first offset for each of the given partitions.
    fn seek_to_beginning(&self, partitions: &[TopicPartition<'a>]) -> Result<()>;

    /// Seek to the last offset for each of the given partitions.
    fn seek_to_end(&self, partitions: &[TopicPartition<'a>]) -> Result<()>;

    /// Get the offset of the next record that will be fetched (if a record with that offset
    /// exists).
    fn position(&self, partition: &TopicPartition<'a>) -> Result<Option<Offset>>;

    /// Get the last committed offset for the given partition
    /// (whether the commit happened by this process or another).
    /// This offset will be used as the position for the consumer in the event of a failure.
    fn committed(&self, partition: TopicPartition<'a>) -> Committed;

    /// Get the set of partitions that were previously paused by a call to `pause`
    fn paused(&self) -> Vec<TopicPartition<'a>>;

    /// Suspend fetching from the requested partitions.
    fn pause(&self, partitions: &[TopicPartition<'a>]) -> Result<()>;

    /// Resume specified partitions which have been paused with
    fn resume(&self, partitions: &[TopicPartition<'a>]) -> Result<()>;
}

pub type Unsubscribe = LeaveGroup;

pub type Commit = CommitOffset;

pub type Committed = StaticBoxFuture<OffsetAndMetadata>;

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
where
    K: Deserializer,
    V: Deserializer,
    Self: 'static,
{
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

impl<'a, K, V> Consumer for KafkaConsumer<'a, K, V>
where
    K: Deserializer,
    K::Item: Hash,
    V: Deserializer,
    Self: 'static,
{
    type Key = K::Item;
    type Value = V::Item;
    type Topics = ConsumerTopics<'a, K, V>;

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

                    let fetcher = Rc::new(Fetcher::new(
                        inner.client.clone(),
                        subscriptions.clone(),
                        fetch_min_bytes,
                        fetch_max_bytes,
                        fetch_max_wait,
                        partition_fetch_bytes,
                    ));

                    Ok(ConsumerTopics {
                        consumer: KafkaConsumer { inner: inner },
                        subscriptions: subscriptions,
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

pub struct ConsumerTopics<'a, K, V> {
    consumer: KafkaConsumer<'a, K, V>,
    subscriptions: Rc<RefCell<Subscriptions<'a>>>,
    coordinator: ConsumerCoordinator<'a>,
    fetcher: Rc<Fetcher<'a>>,
}


impl<'a, K, V> Subscribed<'a> for ConsumerTopics<'a, K, V>
where
    K: Deserializer,
    K::Item: Hash,
    V: Deserializer,
    Self: 'static,
{
    fn assigment(&self) -> Vec<TopicPartition<'a>> {
        self.subscriptions.borrow().assigned_partitions()
    }

    fn subscription(&self) -> Vec<String> {
        self.subscriptions.borrow().subscription()
    }

    fn unsubscribe(self) -> Unsubscribe {
        self.coordinator.leave_group()
    }

    fn commit(&mut self) -> Commit {
        self.coordinator.commit_offsets()
    }

    fn seek(&self, partition: &TopicPartition<'a>, pos: SeekTo) -> Result<()> {
        self.subscriptions.borrow_mut().seek(partition, pos)
    }

    fn seek_to_beginning(&self, partitions: &[TopicPartition<'a>]) -> Result<()> {
        for partition in partitions {
            self.seek(partition, SeekTo::Beginning)?;
        }
        Ok(())
    }

    fn seek_to_end(&self, partitions: &[TopicPartition<'a>]) -> Result<()> {
        for partition in partitions {
            self.seek(partition, SeekTo::End)?;
        }
        Ok(())
    }

    fn position(&self, partition: &TopicPartition<'a>) -> Result<Option<Offset>> {
        self.subscriptions
            .borrow()
            .assigned_state(&partition)
            .ok_or_else(|| {
                ErrorKind::IllegalArgument(
                    format!("No current assignment for partition {}", partition),
                ).into()
            })
            .map(|state| state.position)
    }

    fn committed(&self, tp: TopicPartition<'a>) -> Committed {
        let topic_name = String::from(tp.topic_name.to_owned());
        let partition_id = tp.partition_id;

        self.coordinator
            .fetch_committed_offsets(vec![tp])
            .and_then(move |offsets| {
                offsets
                    .get(&topic_name)
                    .and_then(|partitions| {
                        partitions
                            .iter()
                            .find(|partition| partition.partition_id == partition_id)
                            .map(move |fetched| {
                                OffsetAndMetadata::with_metadata(
                                    fetched.offset,
                                    fetched.metadata.clone(),
                                )
                            })
                    })
                    .ok_or_else(|| {
                        ErrorKind::NoOffsetForPartition(topic_name, partition_id).into()
                    })
            })
            .static_boxed()
    }

    fn paused(&self) -> Vec<TopicPartition<'a>> {
        self.subscriptions.borrow().paused_partitions()
    }

    fn pause(&self, partitions: &[TopicPartition<'a>]) -> Result<()> {
        for partition in partitions {
            self.subscriptions.borrow_mut().pause(&partition)?;
        }
        Ok(())
    }

    fn resume(&self, partitions: &[TopicPartition<'a>]) -> Result<()> {
        for partition in partitions {
            self.subscriptions.borrow_mut().resume(&partition)?;
        }
        Ok(())
    }
}

impl<'a, K, V> Stream for ConsumerTopics<'a, K, V>
where
    K: Deserializer,
    K::Item: Hash,
    V: Deserializer,
{
    type Item = TopicRecord<K::Item, V::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::NotReady)
    }
}

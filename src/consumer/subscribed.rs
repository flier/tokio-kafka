use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::cmp;
use std::time::Duration;
use std::rc::Rc;

use bytes::IntoBuf;
use futures::{future, Async, Future, Poll, Stream};
use tokio_timer::{Sleep, Timer};

use client::{Client, FetchRecords, FetchedRecords, KafkaClient, StaticBoxFuture, ToStaticBoxFuture};
use consumer::{CommitOffset, ConsumerCoordinator, ConsumerRecord, Coordinator, Fetcher, JoinGroup, KafkaConsumer,
               LeaveGroup, RetrieveOffsets, SeekTo, Subscriptions, UpdatePositions};
use errors::{Error, ErrorKind, Result};
use network::{OffsetAndMetadata, OffsetAndTimestamp, TopicPartition};
use protocol::{FetchOffset, Offset, Timestamp};
use serialization::Deserializer;

/// A trait for to the subscribed list of topics.
pub trait Subscribed<'a> {
    /// Get the set of partitions currently assigned to this consumer.
    fn assigment(&self) -> Vec<TopicPartition<'a>>;

    /// Get the current subscription.
    fn subscription(&self) -> Vec<String>;

    /// Unsubscribe from topics currently subscribed with `Consumer::subscribe`
    fn unsubscribe(&self) -> Unsubscribe;

    /// Commit offsets returned on the last record for all the subscribed list of topics and
    /// partitions.
    fn commit(&self) -> Commit;

    /// Commit the specified offsets for the specified list of topics and
    /// partitions.
    fn commit_offsets<I>(&self, offsets: I) -> Commit
    where
        I: 'static + IntoIterator<Item = (TopicPartition<'a>, OffsetAndMetadata)>;

    /// Overrides the fetch offsets that the consumer will use on the next
    /// record
    fn seek(&self, partition: &TopicPartition<'a>, pos: SeekTo) -> Result<()>;

    /// Seek to the first offset for each of the given partitions.
    fn seek_to_beginning<I>(&self, partitions: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a TopicPartition<'a>>,
    {
        for partition in partitions {
            self.seek(partition, SeekTo::Beginning)?;
        }
        Ok(())
    }

    /// Seek to the last offset for each of the given partitions.
    fn seek_to_end<I>(&self, partitions: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a TopicPartition<'a>>,
    {
        for partition in partitions {
            self.seek(partition, SeekTo::End)?;
        }
        Ok(())
    }

    /// Get the offset of the next record that will be fetched (if a record with that offset
    /// exists).
    fn position(&self, partition: &TopicPartition<'a>) -> Result<Option<Offset>>;

    /// Get the last committed offset for the given partition
    /// (whether the commit happened by this process or another).
    /// This offset will be used as the position for the consumer in the event of a failure.
    fn committed(&self, partition: TopicPartition<'a>) -> Committed;

    /// Get the set of partitions that were previously paused by a call to
    /// `pause`
    fn paused(&self) -> Vec<TopicPartition<'a>>;

    /// Suspend fetching from the requested partitions.
    fn pause<I>(&self, partitions: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a TopicPartition<'a>>;

    /// Resume specified partitions which have been paused with
    fn resume<I>(&self, partitions: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a TopicPartition<'a>>;

    /// Look up the offsets for the given partitions by timestamp.
    fn offsets_for_times(&self, partitions: HashMap<TopicPartition<'a>, Timestamp>) -> OffsetsForTimes<'a>;

    /// Get the first offset for the given partitions.
    fn beginning_offsets(&self, partitions: Vec<TopicPartition<'a>>) -> BeginningOffsets<'a>;

    /// Get the last offset for the given partitions.
    ///
    /// The last offset of a partition is the offset of the upcoming message,
    /// i.e. the offset of the last available message + 1.
    fn end_offsets(&self, partitions: Vec<TopicPartition<'a>>) -> EndOffsets<'a>;
}

pub type Unsubscribe = LeaveGroup;

pub type Commit = CommitOffset;

pub type Committed = StaticBoxFuture<OffsetAndMetadata>;

pub type OffsetsForTimes<'a> = RetrieveOffsets<'a, OffsetAndTimestamp>;

pub type BeginningOffsets<'a> = RetrieveOffsets<'a, Offset>;

pub type EndOffsets<'a> = RetrieveOffsets<'a, Offset>;

#[derive(Clone)]
pub struct SubscribedTopics<'a, K, V>
where
    K: Deserializer,
    V: Deserializer,
{
    inner: Rc<RefCell<Inner<'a, K, V>>>,
}

impl<'a, K, V> SubscribedTopics<'a, K, V>
where
    K: Deserializer,
    V: Deserializer,
    Self: 'static,
{
    pub fn new(
        consumer: KafkaConsumer<'a, K, V>,
        subscriptions: Rc<RefCell<Subscriptions<'a>>>,
        coordinator: Option<ConsumerCoordinator<'a, KafkaClient<'a>>>,
        fetcher: Rc<Fetcher<'a>>,
        timer: Rc<Timer>,
    ) -> Result<SubscribedTopics<'a, K, V>> {
        let state = if let Some(ref coordinator) = coordinator {
            State::Joining(coordinator.join_group())
        } else {
            State::fetching(subscriptions.clone(), fetcher.clone())
        };

        Ok(SubscribedTopics {
            inner: Rc::new(RefCell::new(Inner {
                consumer,
                subscriptions,
                coordinator,
                fetcher,
                timer,
                state,
            })),
        })
    }
}

impl<'a, K, V> Stream for SubscribedTopics<'a, K, V>
where
    K: Deserializer + Clone,
    K::Item: Hash,
    V: Deserializer + Clone,
    Self: 'static,
{
    type Item = ConsumerRecord<'a, K::Item, V::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.inner.borrow_mut().poll()
    }
}

struct Inner<'a, K, V>
where
    K: Deserializer,
    V: Deserializer,
{
    consumer: KafkaConsumer<'a, K, V>,
    subscriptions: Rc<RefCell<Subscriptions<'a>>>,
    coordinator: Option<ConsumerCoordinator<'a, KafkaClient<'a>>>,
    fetcher: Rc<Fetcher<'a>>,
    timer: Rc<Timer>,
    state: State<'a, K::Item, V::Item>,
}

enum State<'a, K, V> {
    Joining(JoinGroup),
    UpdatingOffsets(StaticBoxFuture),
    Updating(UpdatePositions),
    Fetching(FetchRecords),
    Retry(Sleep),
    Fetched(Box<Iterator<Item = ConsumerRecord<'a, K, V>>>, Duration),
}

impl<'a, K, V> State<'a, K, V>
where
    Self: 'static,
{
    fn updating(subscriptions: Rc<RefCell<Subscriptions<'a>>>, fetcher: Rc<Fetcher<'a>>) -> Self {
        let partitions = subscriptions.borrow().assigned_partitions();

        trace!("updating postion of partitions: {:?}", partitions);

        State::Updating(fetcher.update_positions(partitions))
    }

    fn fetching(subscriptions: Rc<RefCell<Subscriptions<'a>>>, fetcher: Rc<Fetcher<'a>>) -> Self {
        let partitions = subscriptions.borrow().fetchable_partitions();

        trace!("fetching records of partitions: {:?}", partitions);

        State::Fetching(fetcher.fetch_records(partitions))
    }

    fn fetched<KD, VD>(
        key_deserializer: KD,
        value_deserializer: VD,
        subscriptions: Rc<RefCell<Subscriptions<'a>>>,
        _auto_commit_enabled: bool,
        throttle_time: Duration,
        records: HashMap<String, Vec<FetchedRecords>>,
    ) -> State<'a, KD::Item, VD::Item>
    where
        KD: 'static + Deserializer + Clone,
        VD: 'static + Deserializer + Clone,
    {
        State::Fetched(
            Box::new(records.into_iter().flat_map(move |(topic_name, records)| {
                let key_deserializer = key_deserializer.clone();
                let value_deserializer = value_deserializer.clone();
                let subscriptions = subscriptions.clone();

                records.into_iter().flat_map(move |record| {
                    let topic_name = topic_name.clone();
                    let partition_id = record.partition_id;
                    let tp = topic_partition!(topic_name.clone(), partition_id);
                    let subscriptions = subscriptions.clone();
                    let key_deserializer = key_deserializer.clone();
                    let value_deserializer = value_deserializer.clone();

                    record.messages.into_iter().map(move |message| {
                        if let Some(state) = subscriptions.borrow_mut().assigned_state_mut(&tp) {
                            state.seek(message.offset + 1);
                        }

                        ConsumerRecord {
                            topic_name: Cow::from(topic_name.clone()),
                            partition_id,
                            offset: message.offset,
                            key: message.key.as_ref().and_then(|buf| {
                                key_deserializer
                                    .clone()
                                    .deserialize(topic_name.as_ref(), &mut buf.into_buf())
                                    .ok()
                            }),
                            value: message.value.as_ref().and_then(|buf| {
                                value_deserializer
                                    .clone()
                                    .deserialize(topic_name.as_ref(), &mut buf.into_buf())
                                    .ok()
                            }),
                            timestamp: message.timestamp.clone(),
                        }
                    })
                })
            })),
            throttle_time,
        )
    }

    fn retry(timer: Rc<Timer>, backoff: Duration) -> Self {
        trace!("request was failed or throttled due to quota violation, {:?}", backoff);

        State::Retry(timer.sleep(backoff))
    }
}

impl<'a, K, V> Stream for Inner<'a, K, V>
where
    K: 'static + Deserializer + Clone,
    K::Item: Hash,
    V: 'static + Deserializer + Clone,
    Self: 'static,
{
    type Item = ConsumerRecord<'a, K::Item, V::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            self.state = match self.state {
                State::Joining(ref mut join_group) => {
                    try_ready!(join_group.poll());

                    if let Some(ref coordinator) = self.coordinator {
                        State::UpdatingOffsets(coordinator.update_offsets())
                    } else {
                        State::updating(self.subscriptions.clone(), self.fetcher.clone())
                    }
                }
                State::UpdatingOffsets(ref mut updating) => {
                    debug!("updating offsets from coordinator");
                    try_ready!(updating.poll());
                    State::updating(self.subscriptions.clone(), self.fetcher.clone())
                }
                State::Updating(ref mut updating) => {
                    try_ready!(updating.poll());

                    State::fetching(self.subscriptions.clone(), self.fetcher.clone())
                }
                State::Retry(ref mut sleep) => {
                    try_ready!(sleep.poll());

                    State::updating(self.subscriptions.clone(), self.fetcher.clone())
                }
                State::Fetching(ref mut fetching) => match fetching.poll() {
                    Ok(Async::Ready((throttle_time, ref records)))
                        if records
                            .iter()
                            .flat_map(|(_, records)| records)
                            .map(|record| record.messages.len())
                            .sum::<usize>() == 0 =>
                    {
                        State::retry(
                            self.timer.clone(),
                            cmp::max(throttle_time, self.consumer.config().fetch_error_backoff()),
                        )
                    }
                    Ok(Async::Ready((throttle_time, records))) => {
                        let key_deserializer = self.consumer.key_deserializer();
                        let value_deserializer = self.consumer.value_deserializer();
                        let auto_commit_enabled = self.consumer.config().auto_commit_enabled;

                        State::<K::Item, V::Item>::fetched(
                            key_deserializer,
                            value_deserializer,
                            self.subscriptions.clone(),
                            auto_commit_enabled,
                            throttle_time,
                            records,
                        )
                    }
                    Ok(Async::NotReady) => {
                        return Ok(Async::NotReady);
                    }
                    Err(err) => {
                        trace!("fail to fetch the records, {}", err);

                        State::retry(self.timer.clone(), self.consumer.config().fetch_error_backoff())
                    }
                },
                State::Fetched(ref mut records, throttle_time) => {
                    if let Some(record) = records.next() {
                        return Ok(Async::Ready(Some(record)));
                    } else if throttle_time > Duration::default() {
                        State::retry(self.timer.clone(), throttle_time)
                    } else {
                        State::fetching(self.subscriptions.clone(), self.fetcher.clone())
                    }
                }
            };
        }
    }
}

impl<'a, K, V> Inner<'a, K, V>
where
    K: Deserializer,
    V: Deserializer,
    Self: 'static,
{
    fn assigment(&self) -> Vec<TopicPartition<'a>> {
        self.subscriptions.borrow().assigned_partitions()
    }

    fn subscription(&self) -> Vec<String> {
        self.subscriptions.borrow().subscription()
    }

    fn unsubscribe(&self) -> Unsubscribe {
        if let Some(ref coordinator) = self.coordinator {
            coordinator.leave_group()
        } else {
            future::ok(()).static_boxed()
        }
    }

    fn commit(&self) -> Commit {
        self.commit_offsets(self.subscriptions.borrow().consumed_partitions())
    }

    fn commit_offsets<I>(&self, offsets: I) -> Commit
    where
        I: 'static + IntoIterator<Item = (TopicPartition<'a>, OffsetAndMetadata)>,
    {
        if let Some(ref coordinator) = self.coordinator {
            coordinator.commit_offsets(offsets)
        } else {
            self.consumer.offset_commit(None, None, None, offsets)
        }
    }

    fn seek(&self, partition: &TopicPartition<'a>, pos: SeekTo) -> Result<()> {
        self.subscriptions.borrow_mut().seek(partition, pos)
    }

    fn position(&self, partition: &TopicPartition<'a>) -> Result<Option<Offset>> {
        self.subscriptions
            .borrow()
            .assigned_state(partition)
            .ok_or_else(|| {
                ErrorKind::IllegalArgument(format!("No current assignment for partition {}", partition)).into()
            })
            .map(|state| state.position)
    }

    fn committed(&self, tp: TopicPartition<'a>) -> Committed {
        let topic_name = String::from(tp.topic_name.to_owned());
        let partition_id = tp.partition_id;

        if let Some(ref coordinator) = self.coordinator {
            coordinator
                .fetch_offsets(vec![tp])
                .and_then(move |mut offsets| {
                    offsets
                        .remove(&topic_name)
                        .and_then(|partitions| {
                            partitions
                                .into_iter()
                                .find(|partition| partition.partition_id == partition_id)
                                .map(move |fetched| {
                                    OffsetAndMetadata::with_metadata(fetched.offset, fetched.metadata.clone())
                                })
                        })
                        .ok_or_else(|| ErrorKind::NoOffsetForPartition(topic_name, partition_id).into())
                })
                .static_boxed()
        } else {
            self.consumer
                .list_offsets(vec![(tp, FetchOffset::Latest)])
                .and_then(move |mut offsets| {
                    offsets
                        .remove(&topic_name)
                        .and_then(|partitions| {
                            partitions
                                .into_iter()
                                .find(|partition| partition.partition_id == partition_id)
                                .and_then(move |fetched| {
                                    fetched.offsets.first().map(|&offset| OffsetAndMetadata::new(offset))
                                })
                        })
                        .ok_or_else(|| ErrorKind::NoOffsetForPartition(topic_name, partition_id).into())
                })
                .static_boxed()
        }
    }

    fn paused(&self) -> Vec<TopicPartition<'a>> {
        self.subscriptions.borrow().paused_partitions()
    }

    fn pause<I>(&self, partitions: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a TopicPartition<'a>>,
    {
        for partition in partitions {
            self.subscriptions.borrow_mut().pause(partition)?;
        }
        Ok(())
    }

    fn resume<I>(&self, partitions: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a TopicPartition<'a>>,
    {
        for partition in partitions {
            self.subscriptions.borrow_mut().resume(partition)?;
        }
        Ok(())
    }
}

impl<'a, K, V> Subscribed<'a> for SubscribedTopics<'a, K, V>
where
    K: Deserializer,
    K::Item: Hash,
    V: Deserializer,
    Self: 'static,
{
    fn assigment(&self) -> Vec<TopicPartition<'a>> {
        self.inner.borrow().assigment()
    }

    fn subscription(&self) -> Vec<String> {
        self.inner.borrow().subscription()
    }

    fn unsubscribe(&self) -> Unsubscribe {
        self.inner.borrow().unsubscribe()
    }

    fn commit(&self) -> Commit {
        self.inner.borrow().commit()
    }

    fn commit_offsets<I>(&self, offsets: I) -> Commit
    where
        I: 'static + IntoIterator<Item = (TopicPartition<'a>, OffsetAndMetadata)>,
    {
        self.inner.borrow().commit_offsets(offsets)
    }

    fn seek(&self, partition: &TopicPartition<'a>, pos: SeekTo) -> Result<()> {
        self.inner.borrow().seek(partition, pos)
    }

    fn position(&self, partition: &TopicPartition<'a>) -> Result<Option<Offset>> {
        self.inner.borrow().position(partition)
    }

    fn committed(&self, tp: TopicPartition<'a>) -> Committed {
        self.inner.borrow().committed(tp)
    }

    fn paused(&self) -> Vec<TopicPartition<'a>> {
        self.inner.borrow().paused()
    }

    fn pause<I>(&self, partitions: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a TopicPartition<'a>>,
    {
        self.inner.borrow().pause(partitions)
    }

    fn resume<I>(&self, partitions: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a TopicPartition<'a>>,
    {
        self.inner.borrow().resume(partitions)
    }

    fn offsets_for_times(&self, partitions: HashMap<TopicPartition<'a>, Timestamp>) -> OffsetsForTimes<'a> {
        self.inner.borrow().fetcher.retrieve_offsets(
            partitions
                .into_iter()
                .map(|(tp, ts)| (tp, FetchOffset::ByTime(ts)))
                .collect(),
        )
    }

    fn beginning_offsets(&self, partitions: Vec<TopicPartition<'a>>) -> BeginningOffsets<'a> {
        self.inner
            .borrow()
            .fetcher
            .retrieve_offsets(partitions.into_iter().map(|tp| (tp, FetchOffset::Earliest)).collect())
    }

    fn end_offsets(&self, partitions: Vec<TopicPartition<'a>>) -> EndOffsets<'a> {
        self.inner
            .borrow()
            .fetcher
            .retrieve_offsets(partitions.into_iter().map(|tp| (tp, FetchOffset::Latest)).collect())
    }
}

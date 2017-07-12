use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

use bytes::IntoBuf;
use futures::{Async, Future, Poll, Stream};

use client::{FetchRecords, StaticBoxFuture, KafkaClient, ToStaticBoxFuture};
use consumer::{CommitOffset, ConsumerCoordinator, ConsumerRecord, Coordinator, Fetcher, JoinGroup,
               KafkaConsumer, LeaveGroup, RetrieveOffsets, SeekTo, Subscriptions, UpdatePositions};
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

    /// Look up the offsets for the given partitions by timestamp.
    fn offsets_for_times(
        &self,
        partitions: HashMap<TopicPartition<'a>, Timestamp>,
    ) -> OffsetsForTimes<'a>;

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

pub struct SubscribedTopics<'a, K, V>
where
    K: Deserializer,
    V: Deserializer,
{
    consumer: KafkaConsumer<'a, K, V>,
    subscriptions: Rc<RefCell<Subscriptions<'a>>>,
    coordinator: ConsumerCoordinator<'a, KafkaClient<'a>>,
    fetcher: Fetcher<'a>,
    state: State<'a, K::Item, V::Item>,
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
        coordinator: ConsumerCoordinator<'a, KafkaClient<'a>>,
        fetcher: Fetcher<'a>,
    ) -> Result<SubscribedTopics<'a, K, V>> {
        let state = State::Joining(coordinator.join_group());

        Ok(SubscribedTopics {
            consumer: consumer,
            subscriptions: subscriptions,
            coordinator: coordinator,
            fetcher: fetcher,
            state: state,
        })
    }
}

enum State<'a, K, V> {
    Joining(JoinGroup),
    Updating(UpdatePositions),
    Fetching(FetchRecords),
    Fetched(Box<Iterator<Item = ConsumerRecord<'a, K, V>>>),
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
        loop {
            let next_state;

            match self.state {
                State::Joining(ref mut join_group) => {
                    match join_group.poll() {
                        Ok(Async::Ready(_)) => {
                            let partitions = self.subscriptions.borrow().assigned_partitions();

                            trace!("updating postion of partitions: {:?}", partitions);

                            next_state = State::Updating(self.fetcher.update_positions(partitions))
                        }
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        }
                        Err(err) => {
                            warn!("fail to join the group, {}", err);

                            return Err(err);
                        }
                    }
                }
                State::Updating(ref mut update_positions) => {
                    match update_positions.poll() {
                        Ok(Async::Ready(_)) => {
                            let partitions = self.subscriptions.borrow().assigned_partitions();

                            trace!("fetching records of partitions: {:?}", partitions);

                            next_state = State::Fetching(self.fetcher.fetch_records(partitions))
                        }
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        }
                        Err(err) => {
                            warn!("fail to update positions, {}", err);

                            return Err(err);
                        }
                    }
                }
                State::Fetching(ref mut fetch) => {
                    match fetch.poll() {
                        Ok(Async::Ready(records)) => {
                            let key_deserializer = self.consumer.key_deserializer();
                            let value_deserializer = self.consumer.value_deserializer();

                            next_state = State::Fetched(Box::new(records.into_iter().flat_map(
                                move |(topic_name, records)| {
                                    let key_deserializer = key_deserializer.clone();
                                    let value_deserializer = value_deserializer.clone();

                                    records.into_iter().flat_map(move |record| {
                                        let topic_name = topic_name.clone();
                                        let partition_id = record.partition_id;
                                        let offset = record.fetch_offset;
                                        let key_deserializer = key_deserializer.clone();
                                        let value_deserializer = value_deserializer.clone();

                                        record.messages.into_iter().map(move |message| {
                                            ConsumerRecord {
                                                topic_name: Cow::from(topic_name.clone()),
                                                partition_id: partition_id,
                                                offset: offset,
                                                key: message.key.as_ref().and_then(|buf| {
                                                    key_deserializer
                                                        .clone()
                                                        .deserialize(
                                                            topic_name.as_ref(),
                                                            &mut buf.into_buf(),
                                                        )
                                                        .ok()
                                                }),
                                                value: message.value.as_ref().and_then(|buf| {
                                                    value_deserializer
                                                        .clone()
                                                        .deserialize(
                                                            topic_name.as_ref(),
                                                            &mut buf.into_buf(),
                                                        )
                                                        .ok()
                                                }),
                                                timestamp: message.timestamp.clone(),
                                            }
                                        })
                                    })
                                },
                            )))
                        }
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        }
                        Err(err) => {
                            warn!("fail to fetch the records, {}", err);

                            return Err(err);
                        }
                    }
                }
                State::Fetched(ref mut records) => {
                    if let Some(record) = records.next() {
                        return Ok(Async::Ready(Some(record)));
                    } else {
                        let partitions = self.subscriptions.borrow().assigned_partitions();

                        next_state = State::Fetching(self.fetcher.fetch_records(partitions))
                    }
                }
            }

            self.state = next_state;
        }
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
            .assigned_state(partition)
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
            .fetch_offsets(vec![tp])
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
            self.subscriptions.borrow_mut().pause(partition)?;
        }
        Ok(())
    }

    fn resume(&self, partitions: &[TopicPartition<'a>]) -> Result<()> {
        for partition in partitions {
            self.subscriptions.borrow_mut().resume(partition)?;
        }
        Ok(())
    }

    fn offsets_for_times(
        &self,
        partitions: HashMap<TopicPartition<'a>, Timestamp>,
    ) -> OffsetsForTimes<'a> {
        self.fetcher.retrieve_offsets(
            partitions
                .into_iter()
                .map(|(tp, ts)| (tp, FetchOffset::ByTime(ts)))
                .collect(),
        )
    }

    fn beginning_offsets(&self, partitions: Vec<TopicPartition<'a>>) -> BeginningOffsets<'a> {
        self.fetcher.retrieve_offsets(
            partitions
                .into_iter()
                .map(|tp| (tp, FetchOffset::Earliest))
                .collect(),
        )
    }

    fn end_offsets(&self, partitions: Vec<TopicPartition<'a>>) -> EndOffsets<'a> {
        self.fetcher.retrieve_offsets(
            partitions
                .into_iter()
                .map(|tp| (tp, FetchOffset::Latest))
                .collect(),
        )
    }
}

use std::cell::RefCell;
use std::collections::HashMap;
use std::iter::IntoIterator;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use futures::{Async, Future, Poll};

use client::{Client, FetchRecords, KafkaClient, ListOffsets, PartitionData, StaticBoxFuture, ToStaticBoxFuture};
use consumer::{OffsetResetStrategy, SeekTo, Subscriptions};
use errors::{Error, ErrorKind};
use network::TopicPartition;
use protocol::{FetchOffset, IsolationLevel, KafkaCode, Offset};

pub struct Fetcher<'a> {
    client: KafkaClient<'a>,
    subscriptions: Rc<RefCell<Subscriptions<'a>>>,
    fetch_min_bytes: usize,
    fetch_max_bytes: usize,
    fetch_max_wait: Duration,
    partition_fetch_bytes: usize,
    isolation_level: IsolationLevel,
}

impl<'a> Fetcher<'a>
where
    Self: 'static,
{
    pub fn new(
        client: KafkaClient<'a>,
        subscriptions: Rc<RefCell<Subscriptions<'a>>>,
        fetch_min_bytes: usize,
        fetch_max_bytes: usize,
        fetch_max_wait: Duration,
        partition_fetch_bytes: usize,
        isolation_level: IsolationLevel,
    ) -> Self {
        Fetcher {
            client,
            subscriptions,
            fetch_min_bytes,
            fetch_max_bytes,
            fetch_max_wait,
            partition_fetch_bytes,
            isolation_level,
        }
    }

    /// Update the fetch positions for the provided partitions.
    pub fn update_positions<I>(&self, partitions: I) -> UpdatePositions
    where
        I: IntoIterator<Item = TopicPartition<'a>>,
    {
        let default_reset_strategy = self.subscriptions.borrow().default_reset_strategy();

        self.reset_offsets(partitions.into_iter().flat_map(|tp| {
            self.subscriptions
                .borrow_mut()
                .assigned_state_mut(&tp)
                .and_then(|state| {
                    if let Some(reset_strategy) = state.reset_strategy {
                        trace!("resetting offset of partition {} to strategy: {:?}", tp, reset_strategy);

                        Some(tp)
                    } else if let Some(offset) = state.committed.as_ref().map(|committed| committed.offset) {
                        trace!(
                            "resetting offset of partition {} to the committed offset {}",
                            tp,
                            offset
                        );

                        state.seek(offset);

                        None
                    } else {
                        trace!(
                            "resetting offset of for partition {} to default strategy: {:?}",
                            tp,
                            default_reset_strategy
                        );

                        state.need_offset_reset(default_reset_strategy);

                        Some(tp)
                    }
                })
        }))
    }

    /// Reset offsets for the given partition using the offset reset strategy.
    fn reset_offsets<I>(&self, partitions: I) -> ResetOffsets
    where
        I: IntoIterator<Item = TopicPartition<'a>>,
    {
        let offset_resets = partitions
            .into_iter()
            .flat_map(|tp| {
                self.subscriptions
                    .borrow()
                    .assigned_state(&tp)
                    .and_then(|state| match state.reset_strategy {
                        Some(OffsetResetStrategy::Earliest) => Some((tp, FetchOffset::Earliest)),
                        Some(OffsetResetStrategy::Latest) => Some((tp, FetchOffset::Latest)),
                        _ => None,
                    })
            })
            .collect::<Vec<_>>();

        let subscriptions = self.subscriptions.clone();

        self.client
            .list_offsets(offset_resets)
            .and_then(move |offsets| {
                for (topic_name, partitions) in offsets {
                    for partition in partitions {
                        let tp = topic_partition!(topic_name.clone(), partition.partition_id);

                        let partition_offset = subscriptions.borrow().assigned_state(&tp).and_then(|state| match state
                            .reset_strategy
                        {
                            Some(OffsetResetStrategy::Earliest) => partition.earliest(),
                            Some(OffsetResetStrategy::Latest) => partition.latest(),
                            _ => partition.offset(),
                        });

                        if let Some(offset) = partition_offset {
                            match partition.error_code {
                                KafkaCode::None => subscriptions.borrow_mut().seek(&tp, SeekTo::Position(offset))?,
                                _ => bail!(ErrorKind::KafkaError(partition.error_code)),
                            }
                        } else {
                            bail!(ErrorKind::KafkaError(KafkaCode::OffsetOutOfRange));
                        }
                    }
                }

                Ok(())
            })
            .static_boxed()
    }

    pub fn reset_offsets_if_needed(&self) -> Option<ResetOffsets> {
        let partitions = self.subscriptions.borrow().partitions_needing_reset();

        if partitions.is_empty() {
            None
        } else {
            Some(self.reset_offsets(partitions))
        }
    }

    /// Set-up a fetch request for any node that we have assigned partitions.
    pub fn fetch_records<I>(&self, partitions: I) -> FetchRecords
    where
        I: IntoIterator<Item = TopicPartition<'a>>,
    {
        let subscriptions = self.subscriptions.clone();
        let default_reset_strategy = self.subscriptions.borrow().default_reset_strategy();

        let fetch_partitions = partitions
            .into_iter()
            .flat_map(|tp| {
                subscriptions.borrow().assigned_state(&tp).map(|state| {
                    let fetch_data = PartitionData {
                        offset: state.position.unwrap(),
                        max_bytes: Some(self.partition_fetch_bytes as i32),
                    };

                    (tp, fetch_data)
                })
            })
            .collect();

        self.client
            .fetch_records(
                self.fetch_max_wait,
                self.fetch_min_bytes,
                self.fetch_max_bytes,
                self.isolation_level,
                fetch_partitions,
            )
            .and_then(move |(throttle_time, records)| {
                for (topic_name, records) in &records {
                    for record in records {
                        let tp = topic_partition!(topic_name.clone(), record.partition_id);

                        if let Some(mut state) = subscriptions.borrow_mut().assigned_state_mut(&tp) {
                            if !state.is_fetchable() {
                                debug!("ignoring fetched records for {} since it is no longer fetchable", tp);
                            } else {
                                match record.error_code {
                                    KafkaCode::None => {
                                        if state.position != Some(record.fetch_offset) {
                                            debug!("discarding stale fetch response for {} since its offset {} does not match the expected offset {:?}", tp, record.fetch_offset, state.position);
                                            continue;
                                        }

                                        state.high_watermark = record.high_watermark;
                                    }
                                    KafkaCode::OffsetOutOfRange => {
                                        if state.position != Some(record.fetch_offset) {
                                            debug!("discarding stale fetch response for {} since its offset {} does not match the expected offset {:?}", tp, record.fetch_offset, state.position);
                                        } else {
                                            state.need_offset_reset(default_reset_strategy);
                                        }
                                    }
                                    _ => bail!(ErrorKind::KafkaError(record.error_code)),
                                }
                            }
                        }
                    }
                }

                Ok((throttle_time, records))
            })
            .static_boxed()
    }

    pub fn retrieve_offsets<T>(&self, partitions: Vec<(TopicPartition<'a>, FetchOffset)>) -> RetrieveOffsets<'a, T> {
        RetrieveOffsets::new(self.client.list_offsets(partitions))
    }
}

pub type ResetOffsets = StaticBoxFuture;

pub type UpdatePositions = StaticBoxFuture;

pub struct RetrieveOffsets<'a, T: 'a> {
    offsets: ListOffsets,
    phantom: PhantomData<&'a T>,
}

impl<'a, T> RetrieveOffsets<'a, T> {
    pub fn new(offsets: ListOffsets) -> RetrieveOffsets<'a, T> {
        RetrieveOffsets {
            offsets,
            phantom: PhantomData,
        }
    }
}

impl<'a> Future for RetrieveOffsets<'a, Offset> {
    type Item = HashMap<TopicPartition<'a>, Offset>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.offsets.poll() {
            Ok(Async::Ready(offsets)) => Ok(Async::Ready(
                offsets
                    .into_iter()
                    .flat_map(|(topic_name, partitions)| {
                        partitions.into_iter().flat_map(move |listed| {
                            listed
                                .offset()
                                .map(|offset| (topic_partition!(topic_name.clone(), listed.partition_id), offset))
                        })
                    })
                    .collect(),
            )),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err),
        }
    }
}

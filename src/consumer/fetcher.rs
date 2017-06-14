use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use futures::Future;

use client::{Client, KafkaClient, StaticBoxFuture, ToStaticBoxFuture};
use consumer::{OffsetResetStrategy, Subscriptions};
use errors::ErrorKind;
use network::TopicPartition;
use protocol::{FetchOffset, KafkaCode};

pub struct Fetcher<'a> {
    client: KafkaClient<'a>,
    subscriptions: Rc<RefCell<Subscriptions<'a>>>,
    fetch_min_bytes: usize,
    fetch_max_bytes: usize,
    fetch_max_wait: Duration,
}

impl<'a> Fetcher<'a>
    where Self: 'static
{
    pub fn new(client: KafkaClient<'a>,
               subscriptions: Rc<RefCell<Subscriptions<'a>>>,
               fetch_min_bytes: usize,
               fetch_max_bytes: usize,
               fetch_max_wait: Duration)
               -> Self {
        Fetcher {
            client: client,
            subscriptions: subscriptions,
            fetch_min_bytes: fetch_min_bytes,
            fetch_max_bytes: fetch_max_bytes,
            fetch_max_wait: fetch_max_wait,
        }
    }

    /// Update the fetch positions for the provided partitions.
    pub fn update_positions(&self, partitions: &[TopicPartition<'a>]) -> UpdatePositions {
        let default_reset_strategy = self.subscriptions.borrow().default_reset_strategy();

        self.reset_offsets(partitions.iter().flat_map(|tp| {
            self.subscriptions
                .borrow_mut()
                .assigned_state_mut(tp)
                .and_then(|state| if state.is_offset_reset_needed() {
                              Some(tp.clone())
                          } else if state.has_committed() {
                              state.need_offset_reset(default_reset_strategy);

                              Some(tp.clone())
                          } else {
                              let committed = state
                                  .committed
                                  .as_ref()
                                  .map_or(0, |committed| committed.offset);

                              debug!("Resetting offset for partition {} to the committed offset {}",
                                     tp,
                                     committed);

                              state.seek(committed);

                              None
                          })
        }))
    }

    /// Reset offsets for the given partition using the offset reset strategy.
    fn reset_offsets<I>(&self, partitions: I) -> ResetOffsets
        where I: Iterator<Item = TopicPartition<'a>>
    {
        let mut offset_resets = Vec::new();

        for tp in partitions {
            if let Some(state) = self.subscriptions.borrow().assigned_state(&tp) {
                match state.reset_strategy {
                    Some(OffsetResetStrategy::Earliest) => {
                        offset_resets.push((tp, FetchOffset::Earliest));
                    }
                    Some(OffsetResetStrategy::Latest) => {
                        offset_resets.push((tp, FetchOffset::Latest));
                    }
                    _ => {
                        return ErrorKind::NoOffsetForPartition(tp.topic_name.into(), tp.partition)
                            .into();
                    }
                }
            }
        }

        let subscriptions = self.subscriptions.clone();

        self.client
            .list_offsets(offset_resets)
            .and_then(move |offsets| {
                for (topic_name, partitions) in offsets {
                    for partition in partitions {
                        if let Some(offset) = partition.offset() {
                            let tp = topic_partition!(topic_name.clone(), partition.partition);

                            subscriptions.borrow_mut().seek(&tp, offset);
                        } else {
                            bail!(ErrorKind::KafkaError(KafkaCode::OffsetOutOfRange));
                        }
                    }
                }

                Ok(())
            })
            .static_boxed()
    }

    /// Set-up a fetch request for any node that we have assigned partitions.
    pub fn fetch_records(&self) {}
}

pub type ResetOffsets = StaticBoxFuture;

pub type UpdatePositions = StaticBoxFuture;

use std::rc::Rc;
use std::cell::RefCell;

use futures::{Future, future};

use errors::ErrorKind;
use protocol::FetchOffset;
use network::TopicPartition;
use client::{Client, KafkaClient, StaticBoxFuture, ToStaticBoxFuture};
use consumer::{OffsetResetStrategy, Subscriptions};

pub struct Fetcher<'a> {
    client: KafkaClient<'a>,
    subscriptions: Rc<RefCell<Subscriptions<'a>>>,
}

impl<'a> Fetcher<'a>
    where Self: 'static
{
    pub fn new(client: KafkaClient<'a>, subscriptions: Rc<RefCell<Subscriptions<'a>>>) -> Self {
        Fetcher {
            client: client,
            subscriptions: subscriptions,
        }
    }

    /// Update the fetch positions for the provided partitions.
    pub fn update_positions(&self, partitions: &[TopicPartition<'a>]) -> UpdatePositions {
        let default_reset_strategy = self.subscriptions.borrow().default_reset_strategy();

        self.reset_offsets(partitions
                               .iter()
                               .flat_map(|tp| {
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
                        let err = ErrorKind::NoOffsetForPartition(tp.topic_name.into(),
                                                                  tp.partition);
                        return ResetOffsets::err(err.into());
                    }
                }
            }
        }

        self.client
            .list_offsets(offset_resets)
            .and_then(|offsets| future::ok(()))
            .static_boxed()
    }
}

pub type ResetOffsets = StaticBoxFuture;

pub type UpdatePositions = StaticBoxFuture;

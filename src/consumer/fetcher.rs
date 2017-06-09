use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use errors::{ErrorKind, Result};
use protocol::FetchOffset;
use network::TopicPartition;
use consumer::{OffsetResetStrategy, Subscriptions};

pub struct Fetcher<'a> {
    subscriptions: Rc<RefCell<Subscriptions<'a>>>,
}

impl<'a> Fetcher<'a> {
    pub fn new(subscriptions: Rc<RefCell<Subscriptions<'a>>>) -> Self {
        Fetcher { subscriptions: subscriptions }
    }

    /// Update the fetch positions for the provided partitions.
    pub fn update_fetch_positions(&self, partitions: &[TopicPartition<'a>]) -> Result<()> {
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
    fn reset_offsets<I>(&self, partitions: I) -> Result<()>
        where I: Iterator<Item = TopicPartition<'a>>
    {
        let mut offset_resets = HashMap::new();

        for tp in partitions {
            if let Some(state) = self.subscriptions.borrow().assigned_state(&tp) {
                match state.reset_strategy {
                    Some(OffsetResetStrategy::Earliest) => {
                        offset_resets.insert(tp, FetchOffset::Earliest);
                    }
                    Some(OffsetResetStrategy::Latest) => {
                        offset_resets.insert(tp, FetchOffset::Latest);
                    }
                    _ => {
                        bail!(ErrorKind::NoOffsetForPartition(tp.topic_name.into(), tp.partition));
                    }
                }
            }
        }

        Ok(())
    }
}

use std::hash::Hash;
use std::iter::FromIterator;
use std::collections::{HashMap, HashSet};

use errors::{ErrorKind, Result};
use protocol::Offset;
use network::{OffsetAndMetadata, TopicPartition};

#[derive(Debug, Default)]
pub struct Subscriptions<'a> {
    /// the list of topics the user has requested
    subscription: HashSet<String>,

    /// the list of topics the group has subscribed to
    /// (set only for the leader on join group completion)
    group_subscription: HashSet<String>,

    /// the partitions that are currently assigned,
    /// note that the order of partition matters
    assignment: HashMap<TopicPartition<'a>, TopicPartitionState<'a>>,
}

impl<'a> Subscriptions<'a> {
    pub fn new() -> Self {
        Subscriptions {
            subscription: HashSet::new(),
            group_subscription: HashSet::new(),
            assignment: HashMap::new(),
        }
    }

    pub fn with_topics<I: Iterator<Item = S>, S: AsRef<str> + Hash + Eq>(topic_names: I) -> Self {
        let topic_names: Vec<String> = topic_names.map(|s| s.as_ref().to_owned()).collect();

        Subscriptions {
            subscription: HashSet::from_iter(topic_names.iter().cloned()),
            group_subscription: HashSet::from_iter(topic_names.iter().cloned()),
            assignment: HashMap::new(),
        }
    }

    pub fn subscribe<I: Iterator<Item = S>, S: AsRef<str> + Hash + Eq>(&mut self, topic_names: I) {
        let topic_names: Vec<String> = topic_names.map(|s| s.as_ref().to_owned()).collect();
        self.subscription = HashSet::from_iter(topic_names.iter().cloned());
        self.group_subscription = &self.group_subscription | &self.subscription;
    }

    /// Add topics to the current group subscription.
    ///
    /// This is used by the group leader to ensure that it receives metadata updates for all topics
    /// that the group is interested in.
    pub fn group_subscribe<I: Iterator<Item = S>, S: AsRef<str> + Hash + Eq>(&mut self,
                                                                             topic_names: I) {
        self.group_subscription
            .extend(topic_names.map(|s| s.as_ref().to_owned()))
    }

    pub fn topics(&self) -> Vec<&str> {
        Vec::from_iter(self.subscription.iter().map(|s| s.as_ref()))
    }

    /// Change the assignment to the specified partitions returned from the coordinator
    pub fn assign_from_subscribed(&mut self, partitions: Vec<TopicPartition<'a>>) -> Result<()> {
        if let Some(tp) = partitions
               .iter()
               .find(|tp| {
                         !self.subscription
                              .contains(&String::from(tp.topic_name.to_owned()))
                     }) {
            bail!(ErrorKind::IllegalArgument(format!("assigned partition {}#{} for non-subscribed topic",
                                                     tp.topic_name,
                                                     tp.partition)))
        }

        self.assignment =
            HashMap::from_iter(partitions
                                   .into_iter()
                                   .map(|tp| (tp, TopicPartitionState::default())));

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct TopicPartitionState<'a> {
    /// whether this partition has been paused by the user
    pub paused: bool,
    /// last consumed position
    pub position: Offset,
    /// the high watermark from last fetch
    pub hight_watermark: Offset,
    /// last committed position
    pub committed: Option<OffsetAndMetadata<'a>>,
    /// the strategy to use if the offset needs resetting
    pub reset_strategy: Option<OffsetResetStrategy>,
}

#[derive(Debug, Clone)]
pub enum OffsetResetStrategy {
    Latest,
    Earliest,
}

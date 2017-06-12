use std::mem;
use std::hash::Hash;
use std::str::FromStr;
use std::iter::FromIterator;
use std::collections::{HashMap, HashSet};

use errors::{Error, ErrorKind, Result};
use protocol::Offset;
use network::{OffsetAndMetadata, TopicPartition};

#[derive(Debug, Default)]
pub struct Subscriptions<'a> {
    default_reset_strategy: OffsetResetStrategy,

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
    pub fn new(default_reset_strategy: OffsetResetStrategy) -> Self {
        Subscriptions {
            default_reset_strategy: default_reset_strategy,
            subscription: HashSet::new(),
            group_subscription: HashSet::new(),
            assignment: HashMap::new(),
        }
    }

    pub fn with_topics<I, S>(topic_names: I, default_reset_strategy: OffsetResetStrategy) -> Self
        where I: Iterator<Item = S>,
              S: AsRef<str> + Hash + Eq
    {
        let topic_names: Vec<String> = topic_names.map(|s| s.as_ref().to_owned()).collect();

        Subscriptions {
            default_reset_strategy: default_reset_strategy,
            subscription: HashSet::from_iter(topic_names.iter().cloned()),
            group_subscription: HashSet::from_iter(topic_names.iter().cloned()),
            assignment: HashMap::new(),
        }
    }

    pub fn default_reset_strategy(&self) -> OffsetResetStrategy {
        self.default_reset_strategy
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

    pub fn missing_positions(&self) -> Vec<&TopicPartition<'a>> {
        self.assignment
            .iter()
            .filter(|&(_, state)| state.position.is_none())
            .map(|(tp, _)| tp)
            .collect()
    }

    pub fn assigned_state(&self, tp: &TopicPartition<'a>) -> Option<&TopicPartitionState<'a>> {
        self.assignment.get(tp)
    }

    pub fn assigned_state_mut(&mut self,
                              tp: &TopicPartition<'a>)
                              -> Option<&mut TopicPartitionState<'a>> {
        self.assignment.get_mut(tp)
    }

    pub fn seek(&mut self, tp: &TopicPartition<'a>, offset: Offset) -> Option<Offset> {
        self.assignment.get_mut(tp).map(|state| state.seek(offset))
    }
}

#[derive(Debug, Clone, Default)]
pub struct TopicPartitionState<'a> {
    /// whether this partition has been paused by the user
    pub paused: bool,
    /// last consumed position
    pub position: Option<Offset>,
    /// the high watermark from last fetch
    pub hight_watermark: Offset,
    /// last committed position
    pub committed: Option<OffsetAndMetadata<'a>>,
    /// the strategy to use if the offset needs resetting
    pub reset_strategy: Option<OffsetResetStrategy>,
}

impl<'a> TopicPartitionState<'a> {
    pub fn has_valid_position(&self) -> bool {
        self.position.is_some()
    }

    pub fn is_offset_reset_needed(&self) -> bool {
        self.reset_strategy.is_some()
    }

    pub fn has_committed(&self) -> bool {
        self.committed.is_some()
    }

    pub fn need_offset_reset(&mut self,
                             reset_strategy: OffsetResetStrategy)
                             -> Option<OffsetResetStrategy> {
        mem::replace(&mut self.reset_strategy, Some(reset_strategy))
    }

    pub fn seek(&mut self, offset: Offset) -> Offset {
        self.position = Some(offset);
        self.reset_strategy = None;
        offset
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OffsetResetStrategy {
    Latest,
    Earliest,
    None,
}

impl Default for OffsetResetStrategy {
    fn default() -> Self {
        OffsetResetStrategy::Latest
    }
}

impl FromStr for OffsetResetStrategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "latest" => Ok(OffsetResetStrategy::Latest),
            "earliest" => Ok(OffsetResetStrategy::Earliest),
            "none" => Ok(OffsetResetStrategy::None),
            _ => bail!(ErrorKind::UnsupportedOffsetResetStrategy(s.to_owned())),
        }
    }
}

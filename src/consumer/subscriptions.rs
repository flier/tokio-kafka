use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::iter::FromIterator;
use std::mem;
use std::str::FromStr;

use errors::{Error, ErrorKind, Result};
use network::{OffsetAndMetadata, TopicPartition};
use protocol::Offset;

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
    assignment: HashMap<TopicPartition<'a>, TopicPartitionState>,
}

impl<'a> Subscriptions<'a> {
    pub fn new(default_reset_strategy: OffsetResetStrategy) -> Self {
        Subscriptions {
            default_reset_strategy,
            subscription: HashSet::new(),
            group_subscription: HashSet::new(),
            assignment: HashMap::new(),
        }
    }

    pub fn with_topics<I, S>(topic_names: I, default_reset_strategy: OffsetResetStrategy) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let topic_names = HashSet::from_iter(topic_names.into_iter().map(|s| s.into()));

        Subscriptions {
            default_reset_strategy,
            subscription: topic_names.clone(),
            group_subscription: topic_names,
            assignment: HashMap::new(),
        }
    }

    pub fn default_reset_strategy(&self) -> OffsetResetStrategy {
        self.default_reset_strategy
    }

    pub fn subscribe<I, S>(&mut self, topic_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str> + Hash + Eq,
    {
        let topic_names: Vec<String> = topic_names.into_iter().map(|s| s.as_ref().to_owned()).collect();
        self.subscription = HashSet::from_iter(topic_names.iter().cloned());
        self.group_subscription = &self.group_subscription | &self.subscription;
    }

    /// Add topics to the current group subscription.
    ///
    /// This is used by the group leader to ensure that it receives metadata updates for all
    /// topics
    /// that the group is interested in.
    pub fn group_subscribe<I: IntoIterator<Item = S>, S: AsRef<str> + Hash + Eq>(&mut self, topic_names: I) {
        self.group_subscription
            .extend(topic_names.into_iter().map(|s| s.as_ref().to_owned()))
    }

    pub fn topics(&self) -> Vec<&str> {
        Vec::from_iter(self.subscription.iter().map(|s| s.as_ref()))
    }

    /// Change the assignment to the specified partitions returned from the
    /// coordinator
    pub fn assign_from_subscribed(&mut self, partitions: Vec<TopicPartition<'a>>) -> Result<()> {
        if let Some(tp) = partitions
            .iter()
            .find(|tp| !self.subscription.contains(&String::from(tp.topic_name.to_owned())))
        {
            bail!(ErrorKind::IllegalArgument(format!(
                "assigned partition {}#{} for non-subscribed topic",
                tp.topic_name, tp.partition_id
            )))
        }

        self.assignment = HashMap::from_iter(
            partitions
                .into_iter()
                .map(|tp| (tp.clone(), self.assignment.get(&tp).cloned().unwrap_or_default())),
        );

        Ok(())
    }

    pub fn missing_positions(&self) -> Vec<&TopicPartition<'a>> {
        self.assignment
            .iter()
            .filter(|&(_, state)| state.position.is_none())
            .map(|(tp, _)| tp)
            .collect()
    }

    pub fn assigned_state(&self, tp: &TopicPartition<'a>) -> Option<&TopicPartitionState> {
        self.assignment.get(tp)
    }

    pub fn assigned_state_mut(&mut self, tp: &TopicPartition<'a>) -> Option<&mut TopicPartitionState> {
        self.assignment.get_mut(tp)
    }

    pub fn seek(&mut self, tp: &TopicPartition<'a>, pos: SeekTo) -> Result<()> {
        self.assignment
            .get_mut(tp)
            .ok_or_else(|| ErrorKind::IllegalArgument(format!("No current assignment for partition {}", tp)).into())
            .map(|state| match pos {
                SeekTo::Beginning => {
                    state.need_offset_reset(OffsetResetStrategy::Earliest);
                }
                SeekTo::Position(offset) => {
                    state.seek(offset);
                }
                SeekTo::End => {
                    state.need_offset_reset(OffsetResetStrategy::Latest);
                }
            })
    }

    pub fn consumed_partitions(&self) -> Vec<(TopicPartition<'a>, OffsetAndMetadata)> {
        self.assignment
            .iter()
            .flat_map(|(tp, state)| {
                state
                    .position
                    .map(|position| (tp.clone(), offset_and_metadata!(position)))
            })
            .collect()
    }

    pub fn assigned_partitions(&self) -> Vec<TopicPartition<'a>> {
        self.assignment.keys().cloned().collect()
    }

    pub fn fetchable_partitions(&self) -> Vec<TopicPartition<'a>> {
        self.assignment
            .iter()
            .filter(|&(_, state)| state.is_fetchable())
            .map(|(tp, _)| tp.clone())
            .collect()
    }

    pub fn subscription(&self) -> Vec<String> {
        self.subscription.iter().cloned().collect()
    }

    pub fn paused_partitions(&self) -> Vec<TopicPartition<'a>> {
        self.assignment
            .iter()
            .filter(|&(_, state)| state.paused)
            .map(|(tp, _)| tp.clone())
            .collect()
    }

    pub fn pause(&mut self, tp: &TopicPartition<'a>) -> Result<()> {
        self.assignment
            .get_mut(tp)
            .map(|state| {
                state.paused = true;
            })
            .ok_or_else(|| ErrorKind::IllegalArgument(format!("No current assignment for partition {}", tp)).into())
    }

    pub fn resume(&mut self, tp: &TopicPartition<'a>) -> Result<()> {
        self.assignment
            .get_mut(tp)
            .map(|state| {
                state.paused = true;
            })
            .ok_or_else(|| ErrorKind::IllegalArgument(format!("No current assignment for partition {}", tp)).into())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SeekTo {
    Beginning,
    Position(Offset),
    End,
}

impl FromStr for SeekTo {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "earliest" => SeekTo::Beginning,
            "latest" => SeekTo::End,
            _ => SeekTo::Position(s.parse()?),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct TopicPartitionState {
    /// whether this partition has been paused by the user
    pub paused: bool,
    /// last consumed position
    pub position: Option<Offset>,
    /// the high watermark from last fetch
    pub high_watermark: Offset,
    /// last committed position
    pub committed: Option<OffsetAndMetadata>,
    /// the strategy to use if the offset needs resetting
    pub reset_strategy: Option<OffsetResetStrategy>,
}

impl TopicPartitionState {
    pub fn is_fetchable(&self) -> bool {
        !self.paused && self.position.is_some()
    }

    pub fn has_valid_position(&self) -> bool {
        self.position.is_some()
    }

    pub fn is_offset_reset_needed(&self) -> bool {
        self.reset_strategy.is_some()
    }

    pub fn has_committed(&self) -> bool {
        self.committed.is_some()
    }

    pub fn need_offset_reset(&mut self, reset_strategy: OffsetResetStrategy) -> Option<OffsetResetStrategy> {
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

#![cfg_attr(feature = "clippy", allow(while_let_on_iterator))]

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use client::{Cluster, Metadata};
use errors::{Error, Result};
use network::TopicPartition;

/// Strategy for assigning partitions to consumer streams.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssignmentStrategy {
    /// Range partitioning works on a per-topic basis.
    ///
    /// For each topic, we lay out the available partitions in numeric order
    /// and the consumer threads in lexicographic order.
    /// We then divide the number of partitions by the total number of consumer streams
    /// (threads) to determine the number of partitions to assign to each consumer.
    /// If it does not evenly divide, then the first few consumers will have one extra
    /// partition.
    Range,

    /// The round-robin partition assignor lays out all the available partitions
    /// and all the available consumer threads.
    ///
    /// It then proceeds to do a round-robin assignment from partition to consumer thread.
    /// If the subscriptions of all consumer instances are identical,
    /// then the partitions will be uniformly distributed.
    /// (i.e., the partition ownership counts will be within a delta of exactly one across all
    /// consumer threads.)
    ///
    /// Round-robin assignment is permitted only if:
    /// (a) Every topic has the same number of streams within a consumer instance
    /// (b) The set of subscribed topics is identical for every consumer instance within the
    /// group.
    RoundRobin,

    /// The sticky assignor serves two purposes.
    ///
    /// First, it guarantees an assignment that is as balanced as possible, meaning either:
    /// - the numbers of topic partitions assigned to consumers differ by at most one; or
    /// - each consumer that has 2+ fewer topic partitions than some other consumer
    /// cannot get any of those topic partitions transferred to it.
    ///
    /// Second, it preserved as many existing assignment as possible when a reassignment occurs.
    /// This helps in saving some of the overhead processing
    /// when topic partitions move from one consumer to another.
    Sticky,

    /// unsupported custom strategy
    Custom(String),
}

impl AssignmentStrategy {
    pub fn assignor(&self) -> Option<Box<PartitionAssignor>> {
        match *self {
            AssignmentStrategy::Range => Some(Box::new(RangeAssignor::default())),
            AssignmentStrategy::RoundRobin => Some(Box::new(RoundRobinAssignor::default())),
            AssignmentStrategy::Sticky => Some(Box::new(StickyAssignor::default())),
            AssignmentStrategy::Custom(ref strategy) => {
                warn!("unsupported assignment strategy: {}", strategy);

                None
            }
        }
    }
}

impl FromStr for AssignmentStrategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "range" => Ok(AssignmentStrategy::Range),
            "roundrobin" => Ok(AssignmentStrategy::RoundRobin),
            "sticky" => Ok(AssignmentStrategy::Sticky),
            _ => Ok(AssignmentStrategy::Custom(s.to_owned())),
        }
    }
}

/// Define custom partition assignment for use in `KafkaConsumer`
///
/// Members of the consumer group subscribe to the topics they are interested in
/// and forward their subscriptions to a Kafka broker serving as the group coordinator.
/// The coordinator selects one member to perform the group assignment
/// and propagates the subscriptions of all members to it.
/// Then `PartitionAssignor::assign` is called to perform the assignment
/// and the results are forwarded back to each respective members
pub trait PartitionAssignor {
    /// Unique name for this assignor
    fn name(&self) -> &'static str;

    /// strategy for this assignor
    fn strategy(&self) -> AssignmentStrategy;

    /// Return a serializable object representing the local member's
    /// subscription.
    fn subscription<'a>(&self, topics: Vec<Cow<'a, str>>) -> Subscription<'a> {
        Subscription {
            topics: topics,
            user_data: None,
        }
    }

    /// Perform the group assignment given the member subscriptions and current cluster
    /// metadata.
    fn assign<'a>(
        &self,
        metadata: &'a Metadata,
        subscriptions: HashMap<Cow<'a, str>, Subscription<'a>>,
    ) -> HashMap<Cow<'a, str>, Assignment<'a>>;
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Subscription<'a> {
    pub topics: Vec<Cow<'a, str>>,
    pub user_data: Option<Cow<'a, [u8]>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Assignment<'a> {
    pub partitions: Vec<TopicPartition<'a>>,
    pub user_data: Option<Cow<'a, [u8]>>,
}

/// The range assignor works on a per-topic basis.
///
/// For each topic, we lay out the available partitions in numeric order and the consumers in
/// lexicographic order.
/// We then divide the number of partitions by the total number of consumers to determine
/// the number of partitions to assign to each consumer.
/// If it does not evenly divide, then the first few consumers will have one extra partition.
///
/// For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic
/// has 3 partitions,
/// resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
///
/// The assignment will be:
/// C0: [t0p0, t0p1, t1p0, t1p1]
/// C1: [t0p2, t1p2]
#[derive(Debug, Default)]
pub struct RangeAssignor {}

impl PartitionAssignor for RangeAssignor {
    fn name(&self) -> &'static str {
        "range"
    }

    fn strategy(&self) -> AssignmentStrategy {
        AssignmentStrategy::Range
    }

    fn assign<'a>(
        &self,
        metadata: &'a Metadata,
        subscriptions: HashMap<Cow<'a, str>, Subscription<'a>>,
    ) -> HashMap<Cow<'a, str>, Assignment<'a>> {
        let mut consumers_per_topic = HashMap::new();

        for (member_id, subscription) in subscriptions {
            for topic_name in subscription.topics {
                consumers_per_topic
                    .entry(topic_name)
                    .or_insert_with(Vec::new)
                    .push(member_id.clone());
            }
        }

        let mut assignment = HashMap::new();

        let mut topic_names: Vec<Cow<'a, str>> = consumers_per_topic.keys().cloned().collect();

        topic_names.sort();

        for topic_name in topic_names {
            if let (Some(mut partitions), Some(mut consumers)) = (
                metadata.partitions_for_topic(&topic_name),
                consumers_per_topic.get_mut(&topic_name),
            ) {
                consumers.sort();

                let partitions_per_consumer = partitions.len() / consumers.len();
                let consumers_with_extra_partition = partitions.len() % consumers.len();

                for (i, member_id) in consumers.iter().enumerate() {
                    let remaining = partitions
                        .split_off(partitions_per_consumer + if i >= consumers_with_extra_partition { 0 } else { 1 });

                    assignment
                        .entry(member_id.clone())
                        .or_insert_with(Assignment::default)
                        .partitions
                        .append(&mut partitions);

                    partitions = remaining;
                }
            }
        }

        assignment
    }
}

/// The round robin assignor lays out all the available partitions and all the available consumers.
///
/// It then proceeds to do a round robin assignment from partition to consumer. If the
/// subscriptions of all consumer
/// instances are identical, then the partitions will be uniformly distributed. (i.e., the
/// partition ownership counts
/// will be within a delta of exactly one across all consumers.)
///
/// For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic
/// has 3 partitions,
/// resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
///
/// The assignment will be:
/// C0: [t0p0, t0p2, t1p1]
/// C1: [t0p1, t1p0, t1p2]
///
/// When subscriptions differ across consumer instances, the assignment process still considers each
/// consumer instance in round robin fashion but skips over an instance if it is not subscribed to
/// the topic. Unlike the case when subscriptions are identical, this can result in imbalanced
/// assignments. For example, we have three consumers C0, C1, C2, and three topics t0, t1, t2,
/// with 1, 2, and 3 partitions, respectively. Therefore, the partitions are t0p0, t1p0, t1p1, t2p0,
/// t2p1, t2p2. C0 is subscribed to t0; C1 is subscribed to t0, t1; and C2 is subscribed to t0, t1,
/// t2.
///
/// Tha assignment will be:
/// C0: [t0p0]
/// C1: [t1p0]
/// C2: [t1p1, t2p0, t2p1, t2p2]
#[derive(Debug, Default)]
pub struct RoundRobinAssignor {}

impl PartitionAssignor for RoundRobinAssignor {
    fn name(&self) -> &'static str {
        "roundrobin"
    }

    fn strategy(&self) -> AssignmentStrategy {
        AssignmentStrategy::RoundRobin
    }

    fn assign<'a>(
        &self,
        metadata: &'a Metadata,
        subscriptions: HashMap<Cow<'a, str>, Subscription<'a>>,
    ) -> HashMap<Cow<'a, str>, Assignment<'a>> {
        // get sorted consumers
        let mut consumers: Vec<&Cow<'a, str>> = subscriptions.keys().collect();

        consumers.sort();

        let mut consumers = consumers.iter().cycle();

        // get sorted topic names
        let mut topic_names = HashSet::new();

        topic_names.extend(
            subscriptions
                .values()
                .flat_map(|subscription| subscription.topics.iter().cloned()),
        );

        let mut topic_names: Vec<Cow<'a, str>> = topic_names.into_iter().collect();

        topic_names.sort();

        let mut assignment = HashMap::new();

        for topic_name in topic_names {
            if let Some(partitions) = metadata.partitions_for_topic(&topic_name) {
                for partition in partitions {
                    while let Some(consumer) = consumers.next() {
                        if subscriptions[*consumer].topics.contains(&partition.topic_name) {
                            assignment
                                .entry((*consumer).clone())
                                .or_insert_with(Assignment::default)
                                .partitions
                                .push(partition.clone());

                            break;
                        }
                    }
                }
            }
        }

        assignment
    }
}

/// The sticky assignor serves two purposes.
///
/// First, it guarantees an assignment that is as balanced as possible, meaning either:
/// - the numbers of topic partitions assigned to consumers differ by at most one; or
/// - each consumer that has 2+ fewer topic partitions than some other consumer
/// cannot get any of those topic partitions transferred to it.
///
/// Second, it preserved as many existing assignment as possible when a reassignment occurs.
/// This helps in saving some of the overhead processing when topic partitions move from one
/// consumer to another.
///
/// Starting fresh it would work by distributing the partitions over consumers as evenly as
/// possible.
/// Even though this may sound similar to how round robin assignor works,
/// the second example below shows that it is not.
///
/// During a reassignment it would perform the reassignment in such a way that in the new assignment
/// 1. topic partitions are still distributed as evenly as possible, and
/// 2. topic partitions stay with their previously assigned consumers as much as possible.
/// Of course, the first goal above takes precedence over the second one.
///
/// Example 1. Suppose there are three consumers `C0, `C1`, `C2`,
/// four topics `t0,` `t1`, `t2`, `t3`, and each topic has 2 partitions,
/// resulting in partitions `t0p0`, `t0p1`, `t1p0`, `t1p1`, `t2p0`, `t2p1`, `t3p0`, `t3p1`.
/// Each consumer is subscribed to all three topics.
///
/// The assignment with both sticky and round robin assignors will be:
///
/// - `C0: [t0p0, t1p1, t3p0]`
/// - `C1: [t0p1, t2p0, t3p1]`
/// - `C2: [t1p0, t2p1]`
///
/// Now, let's assume `C1` is removed and a reassignment is about to happen.
/// The round robin assignor would produce:
///
/// - `C0: [t0p0, t1p0, t2p0, t3p0]`
/// - `C2: [t0p1, t1p1, t2p1, t3p1]`
///
/// while the sticky assignor would result in:
///
/// - `C0 [t0p0, t1p1, t3p0, t2p0]`
/// - `C2 [t1p0, t2p1, t0p1, t3p1]`
///
/// preserving all the previous assignments (unlike the round robin assignor).
///
/// Example 2. There are three consumers `C0`, `C1`, `C2`,
/// and three topics `t0`, `t1`, `t2`, with 1, 2, and 3 partitions respectively.
/// Therefore, the partitions are `t0p0`, `t1p0`, `t1p1`, `t2p0`, `t2p1`, `t2p2`.
/// `C0` is subscribed to `t0`; `C1` is subscribed to `t0`, `t1`;
/// and `C2` is subscribed to `t0`, `t1`, `t2`.
///
/// The round robin assignor would come up with the following assignment:
///
/// - `C0 [t0p0]`
/// - `C1 [t1p0]`
/// - `C2 [t1p1, t2p0, t2p1, t2p2]`
///
/// which is not as balanced as the assignment suggested by sticky assignor:
///
/// - `C0 [t0p0]`
/// - `C1 [t1p0, t1p1]`
/// - `C2 [t2p0, t2p1, t2p2]`
///
/// Now, if consumer `C0` is removed, these two assignors would produce the following assignments.
/// Round Robin (preserves 3 partition assignments):
///
/// - `C1 [t0p0, t1p1]`
/// - `C2 [t1p0, t2p0, t2p1, t2p2]`
///
/// Sticky (preserves 5 partition assignments):
///
/// - `C1 [t1p0, t1p1, t0p0]`
/// - `C2 [t2p0, t2p1, t2p2]`
///
#[derive(Debug, Default)]
pub struct StickyAssignor {}

impl PartitionAssignor for StickyAssignor {
    fn name(&self) -> &'static str {
        "sticky"
    }

    fn strategy(&self) -> AssignmentStrategy {
        AssignmentStrategy::Sticky
    }

    fn assign<'a>(
        &self,
        _metadata: &'a Metadata,
        _subscriptions: HashMap<Cow<'a, str>, Subscription<'a>>,
    ) -> HashMap<Cow<'a, str>, Assignment<'a>> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use client::PartitionInfo;

    use super::*;

    /// For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each
    /// topic has 3 partitions,
    /// resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
    ///
    /// The assignment will be:
    /// C0: [t0p0, t0p1, t1p0, t1p1]
    /// C1: [t0p2, t1p2]
    #[test]
    fn test_range_assignor() {
        let assignor = RangeAssignor::default();
        let metadata = Metadata::with_topics(vec![
            (
                "t0".into(),
                vec![PartitionInfo::new(0), PartitionInfo::new(1), PartitionInfo::new(2)],
            ),
            (
                "t1".into(),
                vec![PartitionInfo::new(0), PartitionInfo::new(1), PartitionInfo::new(2)],
            ),
        ]);
        let subscriptions = HashMap::from_iter(
            vec![
                (
                    "c0".into(),
                    Subscription {
                        topics: vec!["t0".into(), "t1".into()],
                        user_data: None,
                    },
                ),
                (
                    "c1".into(),
                    Subscription {
                        topics: vec!["t0".into(), "t1".into()],
                        user_data: None,
                    },
                ),
            ].into_iter(),
        );

        let assignment = assignor.assign(&metadata, subscriptions);

        assert_eq!(assignment.len(), 2);
        assert_eq!(
            assignment["c0"],
            Assignment {
                partitions: vec![
                    topic_partition!("t0", 0),
                    topic_partition!("t0", 1),
                    topic_partition!("t1", 0),
                    topic_partition!("t1", 1),
                ],
                user_data: None,
            }
        );
        assert_eq!(
            assignment["c1"],
            Assignment {
                partitions: vec![topic_partition!("t0", 2), topic_partition!("t1", 2)],
                user_data: None,
            }
        );
    }

    /// For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each
    /// topic has 3 partitions,
    /// resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
    ///
    /// The assignment will be:
    /// C0: [t0p0, t0p2, t1p1]
    /// C1: [t0p1, t1p0, t1p2]
    ///
    #[test]
    fn test_roundrobin_assignor() {
        let assignor = RoundRobinAssignor::default();
        let metadata = Metadata::with_topics(vec![
            (
                "t0".into(),
                vec![PartitionInfo::new(0), PartitionInfo::new(1), PartitionInfo::new(2)],
            ),
            (
                "t1".into(),
                vec![PartitionInfo::new(0), PartitionInfo::new(1), PartitionInfo::new(2)],
            ),
        ]);
        let subscriptions = HashMap::from_iter(
            vec![
                (
                    "c0".into(),
                    Subscription {
                        topics: vec!["t0".into(), "t1".into()],
                        user_data: None,
                    },
                ),
                (
                    "c1".into(),
                    Subscription {
                        topics: vec!["t0".into(), "t1".into()],
                        user_data: None,
                    },
                ),
            ].into_iter(),
        );

        let assignment = assignor.assign(&metadata, subscriptions);

        assert_eq!(assignment.len(), 2);
        assert_eq!(
            assignment["c0"],
            Assignment {
                partitions: vec![
                    topic_partition!("t0", 0),
                    topic_partition!("t0", 2),
                    topic_partition!("t1", 1),
                ],
                user_data: None,
            }
        );
        assert_eq!(
            assignment["c1"],
            Assignment {
                partitions: vec![
                    topic_partition!("t0", 1),
                    topic_partition!("t1", 0),
                    topic_partition!("t1", 2),
                ],
                user_data: None,
            }
        );
    }

    /// For example, we have three consumers C0, C1, C2, and three topics t0, t1, t2,
    /// with 1, 2, and 3 partitions, respectively. Therefore, the partitions are t0p0, t1p0,
    /// t1p1, t2p0,
    /// t2p1, t2p2. C0 is subscribed to t0; C1 is subscribed to t0, t1; and C2 is subscribed to
    /// t0, t1, t2.
    ///
    /// Tha assignment will be:
    /// C0: [t0p0]
    /// C1: [t1p0]
    /// C2: [t1p1, t2p0, t2p1, t2p2]
    #[test]
    fn test_roundrobin_assignor_more() {
        let assignor = RoundRobinAssignor::default();
        let metadata = Metadata::with_topics(vec![
            ("t0".into(), vec![PartitionInfo::new(0)]),
            ("t1".into(), vec![PartitionInfo::new(0), PartitionInfo::new(1)]),
            (
                "t2".into(),
                vec![PartitionInfo::new(0), PartitionInfo::new(1), PartitionInfo::new(2)],
            ),
        ]);
        let subscriptions = HashMap::from_iter(
            vec![
                (
                    "c0".into(),
                    Subscription {
                        topics: vec!["t0".into()],
                        user_data: None,
                    },
                ),
                (
                    "c1".into(),
                    Subscription {
                        topics: vec!["t0".into(), "t1".into()],
                        user_data: None,
                    },
                ),
                (
                    "c2".into(),
                    Subscription {
                        topics: vec!["t0".into(), "t1".into(), "t2".into()],
                        user_data: None,
                    },
                ),
            ].into_iter(),
        );

        let assignment = assignor.assign(&metadata, subscriptions);

        assert_eq!(assignment.len(), 3);
        assert_eq!(
            assignment["c0"],
            Assignment {
                partitions: vec![topic_partition!("t0", 0)],
                user_data: None,
            }
        );
        assert_eq!(
            assignment["c1"],
            Assignment {
                partitions: vec![topic_partition!("t1", 0)],
                user_data: None,
            }
        );
        assert_eq!(
            assignment["c2"],
            Assignment {
                partitions: vec![
                    topic_partition!("t1", 1),
                    topic_partition!("t2", 0),
                    topic_partition!("t2", 1),
                    topic_partition!("t2", 2),
                ],
                user_data: None,
            }
        );
    }
}

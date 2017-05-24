use std::collections::HashMap;

use bytes::Bytes;

use network::TopicPartition;
use client::{Cluster, Metadata};

/// Strategy for assigning partitions to consumer streams.
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssignmentStrategy {
    /// Range partitioning works on a per-topic basis.
    ///
    /// For each topic, we lay out the available partitions in numeric order
    /// and the consumer threads in lexicographic order.
    /// We then divide the number of partitions by the total number of consumer streams (threads)
    /// to determine the number of partitions to assign to each consumer.
    /// If it does not evenly divide, then the first few consumers will have one extra partition.
    Range,

    /// The round-robin partition assignor lays out all the available partitions
    /// and all the available consumer threads.
    ///
    /// It then proceeds to do a round-robin assignment from partition to consumer thread.
    /// If the subscriptions of all consumer instances are identical,
    /// then the partitions will be uniformly distributed.
    /// (i.e., the partition ownership counts will be within a delta of exactly one across all consumer threads.)
    /// Round-robin assignment is permitted only if:
    /// (a) Every topic has the same number of streams within a consumer instance
    /// (b) The set of subscribed topics is identical for every consumer instance within the group.
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
}

impl AssignmentStrategy {
    pub fn assignor(&self) -> Box<PartitionAssignor> {
        match *self {
            AssignmentStrategy::Range => Box::new(RangeAssignor::default()),
            AssignmentStrategy::RoundRobin => Box::new(RoundRobinAssignor::default()),
            AssignmentStrategy::Sticky => Box::new(StickyAssignor::default()),
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

    /// Return a serializable object representing the local member's subscription.
    fn subscription(&self, topics: Vec<String>) -> Subscription {
        Subscription {
            topics: topics,
            user_data: None,
        }
    }

    /// Perform the group assignment given the member subscriptions and current cluster metadata.
    fn assign<'a>(&self,
                  metadata: &'a Metadata,
                  subscriptions: HashMap<String, Subscription>)
                  -> HashMap<String, Assignment<'a>>;
}

#[derive(Debug, Default, PartialEq)]
pub struct Subscription {
    pub topics: Vec<String>,
    pub user_data: Option<Bytes>,
}

#[derive(Debug, Default, PartialEq)]
pub struct Assignment<'a> {
    pub partitions: Vec<TopicPartition<'a>>,
    pub user_data: Option<Bytes>,
}

/// The range assignor works on a per-topic basis.
///
/// For each topic, we lay out the available partitions in numeric order and the consumers in lexicographic order.
/// We then divide the number of partitions by the total number of consumers to determine
/// the number of partitions to assign to each consumer.
/// If it does not evenly divide, then the first few consumers will have one extra partition.
///
/// For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
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

    fn assign<'a>(&self,
                  metadata: &'a Metadata,
                  subscriptions: HashMap<String, Subscription>)
                  -> HashMap<String, Assignment<'a>> {
        let mut consumers_per_topic = HashMap::new();

        for (member_id, subscription) in subscriptions {
            for topic_name in subscription.topics.into_iter() {
                consumers_per_topic
                    .entry(topic_name)
                    .or_insert_with(Vec::new)
                    .push(member_id.clone());
            }
        }

        let mut assignment = HashMap::new();

        let mut topic_names: Vec<String> = consumers_per_topic.keys().cloned().collect();

        topic_names.sort();

        for topic_name in topic_names {
            if let (Some(mut partitions), Some(mut consumers)) =
                (metadata.partitions_for_topic(&topic_name),
                 consumers_per_topic.get_mut(&topic_name)) {
                consumers.sort();

                let partitions_per_consumer = partitions.len() / consumers.len();
                let consumers_with_extra_partition = partitions.len() % consumers.len();

                for (i, member_id) in consumers.iter().enumerate() {
                    let mut remaining = partitions.split_off(partitions_per_consumer +
                                                             if i >=
                                                                consumers_with_extra_partition {
                                                                 0
                                                             } else {
                                                                 1
                                                             });

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

#[derive(Debug, Default)]
pub struct RoundRobinAssignor {}

impl PartitionAssignor for RoundRobinAssignor {
    fn name(&self) -> &'static str {
        "roundrobin"
    }

    fn strategy(&self) -> AssignmentStrategy {
        AssignmentStrategy::RoundRobin
    }

    fn assign<'a>(&self,
                  metadata: &'a Metadata,
                  subscriptions: HashMap<String, Subscription>)
                  -> HashMap<String, Assignment<'a>> {
        let assignments = HashMap::new();

        assignments
    }
}

#[derive(Debug, Default)]
pub struct StickyAssignor {}

impl PartitionAssignor for StickyAssignor {
    fn name(&self) -> &'static str {
        "sticky"
    }

    fn strategy(&self) -> AssignmentStrategy {
        AssignmentStrategy::Sticky
    }

    fn assign<'a>(&self,
                  metadata: &'a Metadata,
                  subscriptions: HashMap<String, Subscription>)
                  -> HashMap<String, Assignment<'a>> {
        let assignments = HashMap::new();

        assignments
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use client::PartitionInfo;

    use super::*;

    #[test]
    fn test_range_assignor() {
        let assignor = RangeAssignor::default();
        let metadata = Metadata::with_topics(vec![("t0".to_owned(),
                                                   vec![PartitionInfo::new(0),
                                                        PartitionInfo::new(1),
                                                        PartitionInfo::new(2)]),
                                                  ("t1".to_owned(),
                                                   vec![PartitionInfo::new(0),
                                                        PartitionInfo::new(1),
                                                        PartitionInfo::new(2)])]);
        let subscriptions =
            HashMap::from_iter(vec![("c0".to_owned(),
                                     Subscription {
                                         topics: vec!["t0".to_owned(), "t1".to_owned()],
                                         user_data: None,
                                     }),
                                    ("c1".to_owned(),
                                     Subscription {
                                         topics: vec!["t0".to_owned(), "t1".to_owned()],
                                         user_data: None,
                                     })]
                                       .into_iter());

        let assignment = assignor.assign(&metadata, subscriptions);

        assert_eq!(assignment.len(), 2);
        assert_eq!(assignment["c0"],
                   Assignment {
                       partitions: vec![topic_partition!("t0", 0),
                                        topic_partition!("t0", 1),
                                        topic_partition!("t1", 0),
                                        topic_partition!("t1", 1)],
                       user_data: None,
                   });
        assert_eq!(assignment["c1"],
                   Assignment {
                       partitions: vec![topic_partition!("t0", 2), topic_partition!("t1", 2)],
                       user_data: None,
                   });
    }
}

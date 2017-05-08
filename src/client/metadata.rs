use std::slice;
use std::borrow::{Borrow, Cow};
use std::collections::hash_map::HashMap;
use std::iter::FromIterator;

use protocol::{MetadataResponse, PartitionId};
use client::{Broker, BrokerRef, Cluster, PartitionInfo, TopicPartition};

#[derive(Debug)]
pub struct Metadata {
    // ~ a list of known brokers referred to by the index in this
    // vector.  This index is also referred to as `BrokerRef` and is
    // enforced by this module.
    //
    // Note: loading of additional topic metadata must preserve
    // already present brokers in this vector at their position.
    // See `KafkaState::update_metadata`
    brokers: Vec<Broker>,

    // ~ a mapping of topic to information about its partitions
    topic_partitions: HashMap<String, TopicPartitions>,

    // ~ a mapping of groups to their coordinators
    group_coordinators: HashMap<String, BrokerRef>,
}

impl Metadata {
    pub fn with_topics(topics: Vec<(String, Vec<PartitionInfo>)>) -> Self {
        Metadata {
            brokers: Vec::new(),
            topic_partitions: HashMap::from_iter(topics
                                                     .into_iter()
                                                     .map(|(topic_name, partitions)| {
                                                              (topic_name,
                                                               TopicPartitions {
                                                                   partitions: partitions,
                                                               })
                                                          })),
            group_coordinators: HashMap::new(),
        }
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Metadata {
            brokers: Vec::new(),
            topic_partitions: HashMap::new(),
            group_coordinators: HashMap::new(),
        }
    }
}

impl Cluster for Metadata {
    fn brokers(&self) -> &[Broker] {
        self.brokers.as_slice()
    }

    fn topics<'a>(&'a self) -> HashMap<Cow<'a, str>, &[PartitionInfo]> {
        HashMap::from_iter(self.topic_partitions
                               .iter()
                               .map(|(topic_name, topic_partitions)| {
                                        (topic_name.as_str().into(), topic_partitions.partitions())
                                    }))
    }

    fn topic_names(&self) -> Vec<&str> {
        self.topic_partitions
            .keys()
            .map(|topic_name| topic_name.as_str())
            .collect()
    }

    fn find_broker(&self, broker_ref: BrokerRef) -> Option<&Broker> {
        self.brokers
            .iter()
            .find(|broker| broker.id() == broker_ref.index())
    }

    fn leader_for(&self, tp: &TopicPartition) -> Option<&Broker> {
        self.find_partition(tp)
            .and_then(|partition| partition.leader())
            .and_then(|leader| self.find_broker(leader))
    }

    fn find_partition(&self, tp: &TopicPartition) -> Option<&PartitionInfo> {
        self.topic_partitions
            .iter()
            .find(|&(topic_name, _)| topic_name.as_str() == tp.topic_name)
            .and_then(|(_, partitions)| {
                          partitions
                              .iter()
                              .find(|&(id, _)| id == tp.partition)
                              .map(|(_, partition)| partition)
                      })
    }

    fn partitions_for_topic<'a>(&'a self, topic_name: String) -> Option<Vec<TopicPartition<'a>>> {
        self.topic_partitions
            .iter()
            .find(|&(topic, _)| topic.as_str() == topic_name)
            .map(|(topic_name, partitions)| {
                partitions
                    .iter()
                    .map(|(id, _)| {
                             TopicPartition {
                                 topic_name: topic_name.as_str().into(),
                                 partition: id,
                             }
                         })
                    .collect()
            })
    }

    fn partitions_for_broker<'a>(&'a self, leader: BrokerRef) -> Vec<TopicPartition<'a>> {
        self.topic_partitions
            .iter()
            .flat_map(|(topic_name, partitions)| {
                partitions
                    .iter()
                    .find(|&(_, partition)| partition.leader() == Some(leader))
                    .map(|(id, _)| {
                             TopicPartition {
                                 topic_name: topic_name.as_str().into(),
                                 partition: id,
                             }
                         })
            })
            .collect()
    }
}

impl From<MetadataResponse> for Metadata {
    fn from(md: MetadataResponse) -> Self {
        Metadata {
            brokers: md.brokers
                .iter()
                .map(|broker| Broker::new(broker.node_id, &broker.host, broker.port as u16))
                .collect(),
            topic_partitions: HashMap::from_iter(md.topics
                                                     .iter()
                                                     .map(|topic| {
                (topic.topic_name.clone(),
                 TopicPartitions {
                     partitions: topic
                         .partitions
                         .iter()
                         .map(|partition| {
                    PartitionInfo {
                        partition: partition.partition,
                        leader: Some(BrokerRef::new(partition.leader)),
                        replicas: partition
                            .replicas
                            .iter()
                            .map(|node| BrokerRef::new(*node))
                            .collect(),
                        in_sync_replicas: partition
                            .isr
                            .iter()
                            .map(|node| BrokerRef::new(*node))
                            .collect(),
                    }
                })
                         .collect(),
                 })
            })),
            group_coordinators: HashMap::new(),
        }
    }
}

/// A representation of partitions for a single topic.
#[derive(Debug)]
pub struct TopicPartitions {
    // ~ This list keeps information about each partition of the
    // corresponding topic - even about partitions currently without a
    // leader.  The index into this list specifies the partition
    // identifier.  (This works due to Kafka numbering partitions 0..N
    // where N is the number of partitions of the topic.)
    partitions: Vec<PartitionInfo>,
}

impl TopicPartitions {
    /// Creates a new partitions vector with all partitions leaderless
    fn new_with_partitions(n: usize) -> Self {
        TopicPartitions { partitions: (0..n).map(|_| PartitionInfo::default()).collect() }
    }

    pub fn partitions(&self) -> &[PartitionInfo] {
        &self.partitions
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    pub fn partition(&self, partition_id: PartitionId) -> Option<&PartitionInfo> {
        self.partitions.get(partition_id as usize)
    }

    pub fn iter(&self) -> PartitionInfoIter {
        self.into_iter()
    }
}

impl<'a> IntoIterator for &'a TopicPartitions {
    type Item = (PartitionId, &'a PartitionInfo);
    type IntoIter = PartitionInfoIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        PartitionInfoIter {
            partition_id: 0,
            iter: self.partitions.iter(),
        }
    }
}

/// An iterator over a topic's partitions.
pub struct PartitionInfoIter<'a> {
    iter: slice::Iter<'a, PartitionInfo>,
    partition_id: PartitionId,
}

impl<'a> Iterator for PartitionInfoIter<'a> {
    type Item = (PartitionId, &'a PartitionInfo);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|tp| {
                     let partition_id = self.partition_id;
                     self.partition_id += 1;
                     (partition_id, tp)
                 })
    }
}

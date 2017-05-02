use std::collections::hash_map::HashMap;
use std::iter::FromIterator;
use std::slice;

use protocol::{BrokerId, MetadataResponse, PartitionId, SupportedApiVersions};

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
    pub fn brokers(&self) -> &[Broker] {
        &self.brokers
    }

    pub fn topics(&self) -> &HashMap<String, TopicPartitions> {
        &self.topic_partitions
    }

    pub fn partitions_for<'a>(&'a self, topic_name: &str) -> Option<&'a TopicPartitions> {
        self.topic_partitions.get(topic_name)
    }

    pub fn find_broker(&self, broker_ref: &BrokerRef) -> Option<&Broker> {
        self.brokers
            .iter()
            .find(|broker| broker.id() == broker_ref.index())
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

impl From<MetadataResponse> for Metadata {
    fn from(md: MetadataResponse) -> Self {
        Metadata {
            brokers: md.brokers
                .iter()
                .map(|broker| {
                         Broker {
                             node_id: broker.node_id,
                             host: broker.host.clone(),
                             port: broker.port as u16,
                             api_versions: None,
                         }
                     })
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
                    TopicPartition {
                        partition: partition.partition,
                        leader: Some(BrokerRef(partition.leader)),
                        replicas: partition
                            .replicas
                            .iter()
                            .map(|node| BrokerRef(*node))
                            .collect(),
                        in_sync_replicas: partition
                            .isr
                            .iter()
                            .map(|node| BrokerRef(*node))
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

/// Describes a Kafka broker node `kafka-rust` is communicating with.
#[derive(Debug)]
pub struct Broker {
    /// The identifier of this broker as understood in a Kafka
    /// cluster.
    node_id: BrokerId,

    /// host of this broker. This information is advertised by
    /// and originating from Kafka cluster itself.
    host: String,

    port: u16,

    /// The version ranges of requests supported by the broker.
    api_versions: Option<SupportedApiVersions>,
}

impl Broker {
    /// Retrives the node_id of this broker as identified with the
    /// remote Kafka cluster.
    #[inline]
    pub fn id(&self) -> BrokerId {
        self.node_id
    }

    /// Retrieves the host:port of the this Kafka broker.
    #[inline]
    pub fn addr(&self) -> (&str, u16) {
        (&self.host, self.port)
    }

    pub fn api_versions(&self) -> Option<&SupportedApiVersions> {
        self.api_versions.as_ref()
    }
}

pub type BrokerIndex = i32;

// See `Brokerref`
static UNKNOWN_BROKER_INDEX: BrokerIndex = ::std::i32::MAX;

/// ~ A custom identifier for a broker.  This type hides the fact that
/// a `TopicPartition` references a `Broker` indirectly, loosely
/// through an index, thereby being able to share broker data without
/// having to fallback to `Rc` or `Arc` or otherwise fighting the
/// borrowck.
// ~ The value `UNKNOWN_BROKER_INDEX` is artificial and represents an
// index to an unknown broker (aka the null value.) Code indexing
// `self.brokers` using a `BrokerRef` _must_ check against this
// constant and/or treat it conditionally.
#[derive(Debug, Copy, Clone)]
pub struct BrokerRef(BrokerIndex);

impl BrokerRef {
    // ~ private constructor on purpose
    fn new(index: BrokerIndex) -> Self {
        BrokerRef(index)
    }

    pub fn index(&self) -> BrokerIndex {
        self.0
    }

    fn set(&mut self, other: BrokerRef) {
        if self.0 != other.0 {
            self.0 = other.0;
        }
    }

    fn set_unknown(&mut self) {
        self.set(BrokerRef::new(UNKNOWN_BROKER_INDEX))
    }
}

impl From<BrokerIndex> for BrokerRef {
    fn from(index: BrokerIndex) -> Self {
        BrokerRef::new(index)
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
    partitions: Vec<TopicPartition>,
}

impl TopicPartitions {
    /// Creates a new partitions vector with all partitions leaderless
    fn new_with_partitions(n: usize) -> TopicPartitions {
        TopicPartitions { partitions: (0..n).map(|_| TopicPartition::default()).collect() }
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    pub fn partition(&self, partition_id: PartitionId) -> Option<&TopicPartition> {
        self.partitions.get(partition_id as usize)
    }

    pub fn iter(&self) -> TopicPartitionIter {
        self.into_iter()
    }
}

impl<'a> IntoIterator for &'a TopicPartitions {
    type Item = (PartitionId, &'a TopicPartition);
    type IntoIter = TopicPartitionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        TopicPartitionIter {
            partition_id: 0,
            iter: self.partitions.iter(),
        }
    }
}

/// Information about a topic-partition.
#[derive(Debug)]
pub struct TopicPartition {
    partition: PartitionId,
    /// The node id of the node currently acting as a leader
    /// for this partition or None if there is no leader
    leader: Option<BrokerRef>,
    replicas: Vec<BrokerRef>,
    in_sync_replicas: Vec<BrokerRef>,
}

impl Default for TopicPartition {
    fn default() -> Self {
        TopicPartition {
            partition: -1,
            leader: None,
            replicas: Vec::new(),
            in_sync_replicas: Vec::new(),
        }
    }
}

impl TopicPartition {
    pub fn new(partition: PartitionId, leader: BrokerRef) -> Self {
        TopicPartition {
            partition: partition,
            leader: Some(leader),
            replicas: vec![],
            in_sync_replicas: vec![],
        }
    }

    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    pub fn leader(&self) -> Option<&BrokerRef> {
        self.leader.as_ref()
    }

    pub fn replicas(&self) -> &[BrokerRef] {
        self.replicas.as_slice()
    }

    pub fn in_sync_replicas(&self) -> &[BrokerRef] {
        self.in_sync_replicas.as_slice()
    }
}

/// An iterator over a topic's partitions.
pub struct TopicPartitionIter<'a> {
    iter: slice::Iter<'a, TopicPartition>,
    partition_id: PartitionId,
}

impl<'a> Iterator for TopicPartitionIter<'a> {
    type Item = (PartitionId, &'a TopicPartition);
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

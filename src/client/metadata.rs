use std::slice;
use std::iter::FromIterator;
use std::collections::hash_map::{HashMap, Keys};

use protocol::{ApiVersion, MetadataResponse};

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

    pub fn topic_partitions(&self) -> &HashMap<String, TopicPartitions> {
        &self.topic_partitions
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
                         .map(|partition| TopicPartition(BrokerRef(partition.leader)))
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
    node_id: i32,

    /// host of this broker. This information is advertised by
    /// and originating from Kafka cluster itself.
    host: String,

    port: u16,

    /// The version ranges of requests supported by the broker.
    api_versions: Option<Vec<ApiVersion>>,
}

impl Broker {
    /// Retrives the node_id of this broker as identified with the
    /// remote Kafka cluster.
    #[inline]
    pub fn id(&self) -> i32 {
        self.node_id
    }

    /// Retrieves the host:port of the this Kafka broker.
    #[inline]
    pub fn addr(&self) -> (&str, u16) {
        (&self.host, self.port)
    }

    pub fn api_versions(&self) -> Option<&[ApiVersion]> {
        self.api_versions.as_ref().map(|ref v| v.as_slice())
    }
}

// See `Brokerref`
static UNKNOWN_BROKER_INDEX: i32 = ::std::i32::MAX;

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
pub struct BrokerRef(i32);

impl BrokerRef {
    // ~ private constructor on purpose
    fn new(index: i32) -> Self {
        BrokerRef(index)
    }

    fn index(&self) -> usize {
        self.0 as usize
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
        TopicPartitions { partitions: (0..n).map(|_| TopicPartition::new()).collect() }
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    pub fn partition(&self, partition_id: i32) -> Option<&TopicPartition> {
        self.partitions.get(partition_id as usize)
    }

    pub fn iter(&self) -> TopicPartitionIter {
        self.into_iter()
    }
}

impl<'a> IntoIterator for &'a TopicPartitions {
    type Item = (i32, &'a TopicPartition);
    type IntoIter = TopicPartitionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        TopicPartitionIter {
            partition_id: 0,
            iter: self.partitions.iter(),
        }
    }
}

/// Metadata for a single topic partition.
#[derive(Debug)]
pub struct TopicPartition(BrokerRef);

impl TopicPartition {
    fn new() -> TopicPartition {
        TopicPartition(BrokerRef::new(UNKNOWN_BROKER_INDEX))
    }
}

/// An iterator over a topic's partitions.
pub struct TopicPartitionIter<'a> {
    iter: slice::Iter<'a, TopicPartition>,
    partition_id: i32,
}

impl<'a> Iterator for TopicPartitionIter<'a> {
    type Item = (i32, &'a TopicPartition);
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

// --------------------------------------------------------------------

// ~ note: this type is re-exported to the crate's public api through
// client::metadata
/// An iterator over the topic names.
pub struct TopicNames<'a> {
    iter: Keys<'a, String, TopicPartitions>,
}

impl<'a> Iterator for TopicNames<'a> {
    type Item = &'a str;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(AsRef::as_ref)
    }
}
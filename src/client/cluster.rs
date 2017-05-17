use std::borrow::Cow;
use std::collections::HashMap;

use protocol::{ApiKeys, ApiVersion, NodeId, PartitionId, UsableApiVersions};
use network::TopicPartition;

/// A representation of a subset of the nodes, topics, and partitions in the Kafka cluster.
pub trait Cluster {
    /// The known set of brokers.
    fn brokers(&self) -> &[Broker];

    /// Get all topics.
    fn topics<'a>(&'a self) -> HashMap<Cow<'a, str>, &[PartitionInfo]>;

    /// Get all topic names.
    fn topic_names(&self) -> Vec<&str>;

    /// Get the broker by the node id (or None if no such node exists)
    fn find_broker(&self, broker: BrokerRef) -> Option<&Broker>;

    /// Get the current leader for the given topic-partition
    fn leader_for(&self, tp: &TopicPartition) -> Option<&Broker>;

    /// Get the metadata for the specified partition
    fn find_partition(&self, tp: &TopicPartition) -> Option<&PartitionInfo>;

    /// Get the list of partitions for this topic
    fn partitions_for_topic<'a>(&'a self, topic_name: &str) -> Option<Vec<TopicPartition<'a>>>;

    /// Get the list of partitions whose leader is this node
    fn partitions_for_broker<'a>(&'a self, broker: BrokerRef) -> Vec<TopicPartition<'a>>;
}

/// Describes a Kafka broker node is communicating with.
#[derive(Debug)]
pub struct Broker {
    /// The identifier of this broker as understood in a Kafka cluster.
    node_id: NodeId,

    /// host of this broker.
    ///
    /// This information is advertised by and originating from Kafka cluster itself.
    host: String,

    /// The port for this node
    port: u16,

    /// The version ranges of requests supported by the broker.
    api_versions: Option<UsableApiVersions>,
}

impl Broker {
    pub fn new(id: NodeId, host: &str, port: u16) -> Self {
        Broker {
            node_id: id,
            host: host.to_owned(),
            port: port,
            api_versions: None,
        }
    }

    /// Retrives the node_id of this broker as identified with the
    /// remote Kafka cluster.
    pub fn id(&self) -> NodeId {
        self.node_id
    }

    pub fn as_ref(&self) -> BrokerRef {
        BrokerRef::new(self.node_id)
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Retrieves the host:port of the this Kafka broker.
    pub fn addr(&self) -> (&str, u16) {
        (&self.host, self.port)
    }

    pub fn api_versions(&self) -> Option<&UsableApiVersions> {
        self.api_versions.as_ref()
    }

    pub fn api_version(&self, api_key: ApiKeys) -> Option<ApiVersion> {
        self.api_versions
            .as_ref()
            .and_then(|api_versions| {
                          api_versions
                              .find(api_key)
                              .map(|api_version| api_version.max_version)
                      })
    }

    pub fn with_api_versions(&self, api_versions: Option<UsableApiVersions>) -> Self {
        Broker {
            node_id: self.node_id,
            host: self.host.clone(),
            port: self.port,
            api_versions: api_versions,
        }
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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct BrokerRef(BrokerIndex);

impl BrokerRef {
    // ~ private constructor on purpose
    pub fn new(index: BrokerIndex) -> Self {
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

/// Information about a topic-partition.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub partition: PartitionId,
    pub leader: Option<BrokerRef>,
    pub replicas: Vec<BrokerRef>,
    pub in_sync_replicas: Vec<BrokerRef>,
}

impl<'a> Default for PartitionInfo {
    fn default() -> Self {
        PartitionInfo {
            partition: -1,
            leader: None,
            replicas: Vec::new(),
            in_sync_replicas: Vec::new(),
        }
    }
}

impl PartitionInfo {
    pub fn new(partition: PartitionId, leader: BrokerRef) -> Self {
        PartitionInfo {
            partition: partition,
            leader: Some(leader),
            replicas: vec![],
            in_sync_replicas: vec![],
        }
    }

    /// The partition id
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// The node id of the node currently acting as a leader for this partition or null if there is no leader
    pub fn leader(&self) -> Option<BrokerRef> {
        self.leader
    }

    /// The complete set of replicas for this partition regardless of whether they are alive or up-to-date
    pub fn replicas(&self) -> &[BrokerRef] {
        self.replicas.as_slice()
    }

    /// The subset of the replicas that are in sync,
    /// that is caught-up to the leader and ready to take over as leader if the leader should fail
    pub fn in_sync_replicas(&self) -> &[BrokerRef] {
        self.in_sync_replicas.as_slice()
    }
}

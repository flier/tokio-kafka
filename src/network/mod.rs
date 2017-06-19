#[macro_use]
mod request;
mod response;
mod codec;
mod resolver;
mod stream;
mod conn;
mod pool;

pub use self::codec::KafkaCodec;
pub use self::conn::{KafkaConnection, KeepAlive, Status};
pub use self::pool::{Pool, Pooled};
pub use self::request::KafkaRequest;
pub use self::resolver::{DnsQuery, DnsResolver, Resolver};
pub use self::response::KafkaResponse;
pub use self::stream::{Connect, KafkaConnector, KafkaStream};

use std::borrow::Cow;
use std::fmt;

use protocol::{Offset, PartitionId, Timestamp};

pub type ConnectionId = u32;

/// A topic name and partition number
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopicPartition<'a> {
    pub topic_name: Cow<'a, str>,
    pub partition_id: PartitionId,
}

impl<'a> fmt::Display for TopicPartition<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}#{}", self.topic_name, self.partition_id)
    }
}

#[macro_export]
macro_rules! topic_partition {
    ($topic_name:expr, $partition_id:expr) => ($crate::network::TopicPartition {
        topic_name: $topic_name.into(),
        partition_id: $partition_id,
    })
}

/// A container class for offset and metadata
///
/// The Kafka offset commit API allows users to provide additional metadata (in the form of a
/// string) when an offset is committed. This can be useful (for example) to store information
/// about which node made the commit, what time the commit was made, etc.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct OffsetAndMetadata {
    /// Message offset to be committed.
    pub offset: Offset,
    /// Any associated metadata the client wants to keep.
    pub metadata: Option<String>,
}

impl OffsetAndMetadata {
    pub fn new(offset: Offset) -> Self {
        OffsetAndMetadata {
            offset: offset,
            metadata: None,
        }
    }

    pub fn with_metadata(offset: Offset, metadata: Option<String>) -> Self {
        OffsetAndMetadata {
            offset: offset,
            metadata: metadata,
        }
    }
}

#[macro_export]
macro_rules! offset_and_metadata {
    ($offset:expr) => ($crate::network::OffsetAndMetadata {
        offset: $offset,
        metadata: None,
    });
    ($offset:expr, $metadata:expr) => ($crate::network::OffsetAndMetadata {
        offset: $offset,
        metadata: Some($metadata.into()),
    });
}

/// A container class for offset and timestamp
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct OffsetAndTimestamp {
    /// Message offset to be committed.
    pub offset: Offset,
    /// Timestamp of the commit
    pub timestamp: Option<Timestamp>,
}

impl OffsetAndTimestamp {
    pub fn new(offset: Offset) -> Self {
        OffsetAndTimestamp {
            offset: offset,
            timestamp: None,
        }
    }

    pub fn with_timestamp(offset: Offset, timestamp: Option<Timestamp>) -> Self {
        OffsetAndTimestamp {
            offset: offset,
            timestamp: timestamp,
        }
    }
}

#[macro_export]
macro_rules! offset_and_timestamp {
    ($offset:expr) => ($crate::network::OffsetAndTimestamp {
        offset: $offset,
        timestamp: None,
    });
    ($offset:expr, $timestamp:expr) => ($crate::network::OffsetAndTimestamp {
        offset: $offset,
        timestamp: Some($timestamp),
    });
}

#[macro_use]
mod request;
mod codec;
mod conn;
mod pool;
mod response;
mod stream;

pub use self::codec::KafkaCodec;
pub use self::conn::{KafkaConnection, KeepAlive, Status};
pub use self::pool::{Pool, Pooled};
pub use self::request::KafkaRequest;
pub use self::response::KafkaResponse;
pub use self::stream::{Connect, KafkaConnector, KafkaStream};

use std::borrow::Cow;
use std::fmt;

use protocol::{Offset, PartitionId, Timestamp};

pub const DEFAULT_PORT: u16 = 9092;

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
        OffsetAndMetadata { offset, metadata: None }
    }

    pub fn with_metadata(offset: Offset, metadata: Option<String>) -> Self {
        OffsetAndMetadata { offset, metadata }
    }
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
            offset,
            timestamp: None,
        }
    }

    pub fn with_timestamp(offset: Offset, timestamp: Option<Timestamp>) -> Self {
        OffsetAndTimestamp { offset, timestamp }
    }
}

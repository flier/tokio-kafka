#[macro_use]
mod request;
mod response;
mod codec;
mod resolver;
mod stream;
mod conn;
mod pool;

pub use self::request::KafkaRequest;
pub use self::response::KafkaResponse;
pub use self::codec::KafkaCodec;
pub use self::resolver::{DnsQuery, DnsResolver, Resolver};
pub use self::stream::{Connect, KafkaConnector, KafkaStream};
pub use self::conn::{KafkaConnection, KeepAlive, Status};
pub use self::pool::{Pool, Pooled};

use std::borrow::Cow;

use protocol::{Offset, PartitionId};

pub type ConnectionId = u32;

/// A topic name and partition number
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopicPartition<'a> {
    pub topic_name: Cow<'a, str>,
    pub partition: PartitionId,
}

#[macro_export]
macro_rules! topic_partition {
    ($topic_name:expr, $partition:expr) => ($crate::network::TopicPartition {
        topic_name: $topic_name.into(),
        partition: $partition,
    })
}

/// A offset and metadata
///
/// The Kafka offset commit API allows users to provide additional metadata (in the form of a string)
/// when an offset is committed. This can be useful (for example) to store information about which
/// node made the commit, what time the commit was made, etc.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct OffsetAndMetadata<'a> {
    pub offset: Offset,
    pub metadata: Option<Cow<'a, str>>,
}

#[macro_export]
macro_rules! offset {
    ($offset:expr) => ($crate::network::OffsetAndMetadata {
        offset: offset,
        metadata: None,
    });
    ($offset:expr, $metadata:expr) => ($crate::network::OffsetAndMetadata {
        offset: offset,
        metadata: $metadata.into(),
    });
}

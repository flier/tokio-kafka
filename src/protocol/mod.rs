#![allow(non_camel_case_types)]

use std::mem;
use std::str::FromStr;
use std::time::Duration;

use time::Timespec;

use errors::{Error, ErrorKind, Result};

mod api_key;
mod code;
mod encode;
#[macro_use]
mod parse;
mod api_versions;
mod fetch;
mod group;
mod header;
mod list_offset;
mod message;
mod metadata;
mod offset_commit;
mod offset_fetch;
mod produce;
mod schema;

pub use self::api_key::{ApiKey, ApiKeys};
pub use self::api_versions::{ApiVersionsRequest, ApiVersionsResponse, UsableApiVersion, UsableApiVersions, SUPPORTED_API_VERSIONS};
pub use self::code::{ErrorCode, KafkaCode};
pub use self::encode::{Encodable, WriteExt, ARRAY_LEN_SIZE, BYTES_LEN_SIZE, OFFSET_SIZE, PARTITION_ID_SIZE,
                       REPLICA_ID_SIZE, STR_LEN_SIZE, TIMESTAMP_SIZE};
pub use self::fetch::{FetchPartition, FetchRequest, FetchResponse, FetchTopic, FetchTopicData,
                      DEFAULT_RESPONSE_MAX_BYTES};
pub use self::group::{DescribeGroupsRequest, DescribeGroupsResponse, GroupCoordinatorRequest,
                      GroupCoordinatorResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupMember,
                      JoinGroupProtocol, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse,
                      ListGroupsRequest, ListGroupsResponse, SyncGroupAssignment, SyncGroupRequest, SyncGroupResponse};
pub use self::header::{parse_response_header, RequestHeader, ResponseHeader};
pub use self::list_offset::{FetchOffset, ListOffsetRequest, ListOffsetResponse, ListPartitionOffset, ListTopicOffset,
                            EARLIEST_TIMESTAMP, LATEST_TIMESTAMP};
pub use self::message::{parse_message_set, Message, MessageSet, MessageSetBuilder, MessageSetEncoder, MessageTimestamp};
pub use self::metadata::{BrokerMetadata, MetadataRequest, MetadataResponse, PartitionMetadata, TopicMetadata};
pub use self::offset_commit::{OffsetCommitPartition, OffsetCommitRequest, OffsetCommitResponse, OffsetCommitTopic};
pub use self::offset_fetch::{OffsetFetchPartition, OffsetFetchRequest, OffsetFetchResponse, OffsetFetchTopic};
pub use self::parse::{display_parse_error, parse_bytes, parse_opt_bytes, parse_opt_str, parse_opt_string, parse_str,
                      parse_string, ParseTag, PARSE_TAGS};
pub use self::produce::{ProducePartitionData, ProduceRequest, ProduceResponse, ProduceTopicData};
pub use self::schema::{Nullable, Schema, SchemaType, VarInt, VarLong};

/// Normal client consumers should always specify this as -1 as they have no node id.
pub const CONSUMER_REPLICA_ID: ReplicaId = -1;
/// The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica
/// broker for debugging purposes.
pub const DEBUGGING_REPLICA_ID: ReplicaId = -2;

pub const DEFAULT_TIMESTAMP: Timestamp = -1;

/// This is a numeric version number for this api.
///
/// We version each API and this version number allows the server to properly interpret the request
/// as the protocol evolves. Responses will always be in the format corresponding to the request
/// version.
pub type ApiVersion = i16;

/// This is a user-supplied integer.
///
/// It will be passed back in the response by the server, unmodified.
///  It is useful for matching request and response between the client and server.
pub type CorrelationId = i32;

/// The partition id.
pub type PartitionId = i32;

/// This is the offset used in kafka as the log sequence number.
pub type Offset = i64;

/// This is the timestamp of the message.
///
/// The timestamp type is indicated in the attributes.
/// Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
pub type Timestamp = i64;

/// The broker id.
pub type NodeId = i32;

/// Broker id of the follower.
pub type ReplicaId = i32;

/// The number of acknowledgments the producer
/// requires the leader to have received before considering a request complete.
pub type RequiredAck = i16;

/// The generation of the group.
pub type GenerationId = i32;

/// Possible choices on acknowledgement requirements when producing/sending messages to Kafka.
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(i16)]
pub enum RequiredAcks {
    /// Indicates to the receiving Kafka broker not to acknowlegde
    /// messages sent to it at all. Sending messages with this
    /// acknowledgement requirement translates into a fire-and-forget
    /// scenario which - of course - is very fast but not reliable.
    None = 0,
    /// Requires the receiving Kafka broker to wait until the sent
    /// messages are written to local disk.  Such messages can be
    /// regarded as acknowledged by one broker in the cluster.
    One = 1,
    /// Requires the sent messages to be acknowledged by all in-sync
    /// replicas of the targeted topic partitions.
    All = -1,
}

impl Default for RequiredAcks {
    fn default() -> Self {
        RequiredAcks::One
    }
}

impl From<RequiredAck> for RequiredAcks {
    fn from(v: RequiredAck) -> Self {
        unsafe { mem::transmute(v) }
    }
}

impl FromStr for RequiredAcks {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "none" => Ok(RequiredAcks::None),
            "one" => Ok(RequiredAcks::One),
            "all" => Ok(RequiredAcks::All),
            _ => bail!(ErrorKind::ParseError(format!("unknown required acks: {}", s),)),
        }
    }
}

pub trait Record {
    fn size(&self, api_version: ApiVersion) -> usize;
}

/// A trait for converting a value to a milliseconds.
pub trait ToMilliseconds {
    fn as_millis(&self) -> u64;
}

impl ToMilliseconds for Duration {
    fn as_millis(&self) -> u64 {
        self.as_secs() * 1000 + u64::from(self.subsec_nanos()) / 1_000_000
    }
}

impl ToMilliseconds for Timespec {
    fn as_millis(&self) -> u64 {
        self.sec as u64 * 1000 + self.nsec as u64 / 1_000_000
    }
}

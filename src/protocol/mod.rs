#![allow(non_camel_case_types)]

use std::mem;
use std::fmt;
use std::time::Duration;
use std::str::FromStr;

use time::Timespec;

use errors::{Error, ErrorKind, Result};

mod encode;
#[macro_use]
mod parse;
mod header;
mod message;
mod produce;
mod fetch;
mod list_offset;
mod metadata;
mod offset_commit;
mod offset_fetch;
mod group;
mod api_versions;

pub use self::encode::{ARRAY_LEN_SIZE, BYTES_LEN_SIZE, Encodable, OFFSET_SIZE, PARTITION_ID_SIZE,
                       REPLICA_ID_SIZE, STR_LEN_SIZE, TIMESTAMP_SIZE, WriteExt};
pub use self::parse::{PARSE_TAGS, ParseTag, display_parse_error, parse_bytes, parse_opt_bytes,
                      parse_opt_str, parse_opt_string, parse_str, parse_string};
pub use self::header::{RequestHeader, ResponseHeader, parse_response_header};
pub use self::message::{Message, MessageSet, MessageSetBuilder, MessageSetEncoder,
                        MessageTimestamp, parse_message_set};
pub use self::produce::{ProducePartitionData, ProduceRequest, ProduceResponse, ProduceTopicData};
pub use self::fetch::{FetchPartition, FetchRequest, FetchResponse, FetchTopic};
pub use self::list_offset::{FetchOffset, ListOffsetRequest, ListOffsetResponse,
                            ListPartitionOffset, ListTopicOffset};
pub use self::metadata::{BrokerMetadata, MetadataRequest, MetadataResponse, PartitionMetadata,
                         TopicMetadata};
pub use self::offset_commit::{OffsetCommitRequest, OffsetCommitResponse};
pub use self::offset_fetch::{OffsetFetchRequest, OffsetFetchResponse};
pub use self::group::{GroupCoordinatorRequest, GroupCoordinatorResponse, HeartbeatRequest,
                      HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
                      LeaveGroupResponse, SyncGroupRequest, SyncGroupResponse};
pub use self::api_versions::{ApiVersionsRequest, ApiVersionsResponse, UsableApiVersion,
                             UsableApiVersions};

pub type ApiKey = i16;
pub type ApiVersion = i16;
pub type CorrelationId = i32;
pub type PartitionId = i32;
pub type ErrorCode = i16;
pub type Offset = i64;
pub type Timestamp = i64;
pub type NodeId = i32;
pub type ReplicaId = i32;
pub type RequiredAck = i16;

/// The following are the numeric codes that the `ApiKey` in the request can take for each of the below request types.
#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(i16)]
pub enum ApiKeys {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    // ConsumerMetadata = 10,
    GroupCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
}

impl ApiKeys {
    pub fn key(&self) -> ApiKey {
        unsafe { mem::transmute(*self) }
    }

    pub fn name(&self) -> &'static str {
        match *self {
            ApiKeys::Produce => "Produce",
            ApiKeys::Fetch => "Fetch",
            ApiKeys::ListOffsets => "ListOffsets",
            ApiKeys::Metadata => "Metadata",
            ApiKeys::LeaderAndIsr => "LeaderAndIsr",
            ApiKeys::StopReplica => "StopReplica",
            ApiKeys::UpdateMetadata => "UpdateMetadata",
            ApiKeys::ControlledShutdown => "ControlledShutdown",
            ApiKeys::OffsetCommit => "OffsetCommit",
            ApiKeys::OffsetFetch => "OffsetFetch",
            ApiKeys::GroupCoordinator => "GroupCoordinator",
            ApiKeys::JoinGroup => "JoinGroup",
            ApiKeys::Heartbeat => "Heartbeat",
            ApiKeys::LeaveGroup => "LeaveGroup",
            ApiKeys::SyncGroup => "SyncGroup",
            ApiKeys::DescribeGroups => "DescribeGroups",
            ApiKeys::ListGroups => "ListGroups",
            ApiKeys::SaslHandshake => "SaslHandshake",
            ApiKeys::ApiVersions => "ApiVersions",
            ApiKeys::CreateTopics => "CreateTopics",
            ApiKeys::DeleteTopics => "DeleteTopics",
        }
    }
}

impl From<ApiKey> for ApiKeys {
    fn from(v: ApiKey) -> Self {
        unsafe { mem::transmute(v) }
    }
}

/// Possible choices on acknowledgement requirements when
/// producing/sending messages to Kafka. See
/// `KafkaClient::produce_messages`.
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
            _ => bail!(ErrorKind::ParseError(format!("unknown required acks: {}", s))),
        }
    }
}

/// Various errors reported by a remote Kafka server.
///
/// We use numeric codes to indicate what problem occurred on the server.
/// These can be translated by the client into exceptions or
/// whatever the appropriate error handling mechanism in the client language.
///
/// See also [Kafka Errors](http://kafka.apache.org/protocol.html)
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(i16)]
pub enum KafkaCode {
    /// The server experienced an unexpected error when processing the request
    Unknown = -1,
    None = 0,
    /// The requested offset is outside the range of offsets
    /// maintained by the server for the given topic/partition
    OffsetOutOfRange = 1,
    /// This indicates that a message contents does not match its CRC
    CorruptMessage = 2,
    /// This request is for a topic or partition that does not exist
    /// on this broker.
    UnknownTopicOrPartition = 3,
    /// The message has a negative size
    InvalidMessageSize = 4,
    /// This error is thrown if we are in the middle of a leadership
    /// election and there is currently no leader for this partition
    /// and hence it is unavailable for writes.
    LeaderNotAvailable = 5,
    /// This error is thrown if the client attempts to send messages
    /// to a replica that is not the leader for some partition. It
    /// indicates that the clients metadata is out of date.
    NotLeaderForPartition = 6,
    /// This error is thrown if the request exceeds the user-specified
    /// time limit in the request.
    RequestTimedOut = 7,
    /// This is not a client facing error and is used mostly by tools
    /// when a broker is not alive.
    BrokerNotAvailable = 8,
    /// If replica is expected on a broker, but is not (this can be
    /// safely ignored).
    ReplicaNotAvailable = 9,
    /// The server has a configurable maximum message size to avoid
    /// unbounded memory allocation. This error is thrown if the
    /// client attempt to produce a message larger than this maximum.
    MessageSizeTooLarge = 10,
    /// Internal error code for broker-to-broker communication.
    StaleControllerEpoch = 11,
    /// If you specify a string larger than configured maximum for
    /// offset metadata
    OffsetMetadataTooLarge = 12,
    /// The server disconnected before a response was received.
    NetworkException = 13,
    /// The broker returns this error code for an offset fetch request
    /// if it is still loading offsets (after a leader change for that
    /// offsets topic partition), or in response to group membership
    /// requests (such as heartbeats) when group metadata is being
    /// loaded by the coordinator.
    GroupLoadInProgress = 14,
    /// The broker returns this error code for group coordinator
    /// requests, offset commits, and most group management requests
    /// if the offsets topic has not yet been created, or if the group
    /// coordinator is not active.
    GroupCoordinatorNotAvailable = 15,
    /// The broker returns this error code if it receives an offset
    /// fetch or commit request for a group that it is not a
    /// coordinator for.
    NotCoordinatorForGroup = 16,
    /// For a request which attempts to access an invalid topic
    /// (e.g. one which has an illegal name), or if an attempt is made
    /// to write to an internal topic (such as the consumer offsets
    /// topic).
    InvalidTopic = 17,
    /// If a message batch in a produce request exceeds the maximum
    /// configured segment size.
    RecordListTooLarge = 18,
    /// Returned from a produce request when the number of in-sync
    /// replicas is lower than the configured minimum and requiredAcks is
    /// -1.
    NotEnoughReplicas = 19,
    /// Returned from a produce request when the message was written
    /// to the log, but with fewer in-sync replicas than required.
    NotEnoughReplicasAfterAppend = 20,
    /// Returned from a produce request if the requested requiredAcks is
    /// invalid (anything other than -1, 1, or 0).
    InvalidRequiredAcks = 21,
    /// Returned from group membership requests (such as heartbeats) when
    /// the generation id provided in the request is not the current
    /// generation.
    IllegalGeneration = 22,
    /// Returned in join group when the member provides a protocol type or
    /// set of protocols which is not compatible with the current group.
    InconsistentGroupProtocol = 23,
    /// Returned in join group when the groupId is empty or null.
    InvalidGroupId = 24,
    /// Returned from group requests (offset commits/fetches, heartbeats,
    /// etc) when the memberId is not in the current generation.
    UnknownMemberId = 25,
    /// Return in join group when the requested session timeout is outside
    /// of the allowed range on the broker
    InvalidSessionTimeout = 26,
    /// Returned in heartbeat requests when the coordinator has begun
    /// rebalancing the group. This indicates to the client that it
    /// should rejoin the group.
    RebalanceInProgress = 27,
    /// This error indicates that an offset commit was rejected because of
    /// oversize metadata.
    InvalidOffsetCommitSize = 28,
    /// Returned by the broker when the client is not authorized to access
    /// the requested topic.
    TopicAuthorizationFailed = 29,
    /// Returned by the broker when the client is not authorized to access
    /// a particular groupId.
    GroupAuthorizationFailed = 30,
    /// Returned by the broker when the client is not authorized to use an
    /// inter-broker or administrative API.
    ClusterAuthorizationFailed = 31,
    /// The timestamp of the message is out of acceptable range.
    InvalidTimestamp = 32,
    /// The broker does not support the requested SASL mechanism.
    UnsupportedSaslMechanism = 33,
    /// Request is not valid given the current SASL state.
    IllegalSaslState = 34,
    /// The version of API is not supported.
    UnsupportedVersion = 35,
    /// Topic with this name already exists.
    TopicAlreadyExists = 36,
    /// Number of partitions is invalid.
    InvalidPartitions = 37,
    /// Replication-factor is invalid.
    InvalidReplicationFactor = 38,
    /// Replica assignment is invalid.
    InvalidReplicaAssignment = 39,
    /// Configuration is invalid.
    InvalidConfig = 40,
    /// This is not the correct controller for this cluster.
    NotController = 41,
    /// This most likely occurs because of a request being malformed by the client library
    /// or the message was sent to an incompatible broker. See the broker logs for more details.
    InvalidRequest = 42,
    /// The message format version on the broker does not support the request.
    UnsupportedForMessageFormat = 43,
    /// Request parameters do not satisfy the configured policy.
    PolicyViolation = 44,
}

impl From<ErrorCode> for KafkaCode {
    fn from(v: ErrorCode) -> Self {
        unsafe { mem::transmute(v) }
    }
}

impl fmt::Display for KafkaCode {
    fn fmt(&self, w: &mut fmt::Formatter) -> fmt::Result {
        write!(w, "{:?}", self)
    }
}

pub trait Record {
    fn size(&self, api_version: ApiVersion) -> usize;
}

pub trait ToMilliseconds {
    fn as_millis(&self) -> u64;
}

impl ToMilliseconds for Duration {
    fn as_millis(&self) -> u64 {
        self.as_secs() * 1000 + self.subsec_nanos() as u64 / 1000_000
    }
}

impl ToMilliseconds for Timespec {
    fn as_millis(&self) -> u64 {
        self.sec as u64 * 1000 + self.nsec as u64 / 1000_000
    }
}

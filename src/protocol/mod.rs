use std::str;

use nom::{be_i16, be_i32};

mod header;
mod message;
mod produce;

pub use self::header::{RequestHeader, ResponseHeader, parse_response_header};
pub use self::message::{Message, MessageSet};
pub use self::produce::{ProduceRequest, ProduceResponse, ProduceTopicData, ProducePartitionData};

/// The following are the numeric codes that the ApiKey in the request can take for each of the below request types.
#[derive(Debug, Copy, Clone)]
#[repr(i16)]
pub enum ApiKeys {
    Produce = 0,
    Fetch = 1,
    Offsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
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

/// Possible choices on acknowledgement requirements when
/// producing/sending messages to Kafka. See
/// `KafkaClient::produce_messages`.
#[derive(Debug, Copy, Clone)]
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

named!(pub parse_str<Option<&str>>,
    do_parse!(
        len: be_i16
     >> s: cond!(len > 0, map_res!(take!(len), str::from_utf8))
     >> (s)
    )
);

named!(pub parse_bytes<Option<&[u8]>>,
    do_parse!(
        len: be_i32
     >> s: cond!(len > 0, take!(len))
     >> (s)
    )
);

#[cfg(test)]
mod tests {
    use nom::{IResult, Needed};

    use super::*;

    #[test]
    fn test_parse_str() {
        assert_eq!(parse_str(b"\0"), IResult::Incomplete(Needed::Size(2)));
        assert_eq!(parse_str(b"\xff\xff"), IResult::Done(&b""[..], None));
        assert_eq!(parse_str(b"\0\0"), IResult::Done(&b""[..], None));
        assert_eq!(parse_str(b"\0\x04test"),
                   IResult::Done(&b""[..], Some("test")));
    }

    #[test]
    fn test_parse_bytes() {
        assert_eq!(parse_bytes(b"\0"), IResult::Incomplete(Needed::Size(4)));
        assert_eq!(parse_bytes(b"\xff\xff\xff\xff"),
                   IResult::Done(&b""[..], None));
        assert_eq!(parse_bytes(b"\0\0\0\0"), IResult::Done(&b""[..], None));
        assert_eq!(parse_bytes(b"\0\0\0\x04test"),
                   IResult::Done(&b""[..], Some(&b"test"[..])));
    }
}
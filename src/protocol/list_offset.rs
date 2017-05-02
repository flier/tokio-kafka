use std::borrow::Cow;
use bytes::{BufMut, ByteOrder, BytesMut};

use time::Timespec;

use nom::{be_i16, be_i32, be_i64};

use errors::Result;
use protocol::{ApiVersion, Encodable, ErrorCode, Offset, ParseTag, PartitionId, ReplicaId,
               RequestHeader, ResponseHeader, Timestamp, WriteExt, parse_response_header,
               parse_string};

pub const LATEST_TIMESTAMP: Timestamp = -1;
pub const EARLIEST_TIMESTAMP: Timestamp = -2;

pub const CONSUMER_REPLICA_ID: ReplicaId = -1;
pub const DEBUGGING_REPLICA_ID: ReplicaId = -2;

/// Possible values when querying a topic's offset.
/// See `KafkaClient::fetch_offsets`.
#[derive(Debug, Copy, Clone)]
pub enum FetchOffset {
    /// Receive the earliest available offset.
    Earliest,
    /// Receive the latest offset.
    Latest,
    /// Used to ask for all messages before a certain time (ms); unix
    /// timestamp in milliseconds.
    /// See https://cwiki.apache.org/confluence/display/KAFKA/Writing+a+Driver+for+Kafka#WritingaDriverforKafka-Offsets
    ByTime(Timespec),
}

impl From<FetchOffset> for Offset {
    fn from(offset: FetchOffset) -> Self {
        match offset {
            FetchOffset::Earliest => -2,
            FetchOffset::Latest => -1,
            FetchOffset::ByTime(t) => t.sec * 1000 + t.nsec as Offset / 1000_000,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListOffsetRequest<'a> {
    pub header: RequestHeader<'a>,
    /// Broker id of the follower. For normal consumers, use -1.
    pub replica_id: ReplicaId,
    /// Topics to list offsets.
    pub topics: Vec<ListTopicOffset<'a>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListTopicOffset<'a> {
    /// The name of the topic.
    pub topic_name: Cow<'a, str>,
    /// Partitions to list offset.
    pub partitions: Vec<ListPartitionOffset>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListPartitionOffset {
    /// The id of the partition the fetch is for.
    pub partition: PartitionId,
    /// Used to ask for all messages before a certain time (ms).
    pub timestamp: Timestamp,
    /// Maximum offsets to return.
    pub max_number_of_offsets: i32,
}

impl<'a> Encodable for ListOffsetRequest<'a> {
    fn encode<T: ByteOrder>(self, dst: &mut BytesMut) -> Result<()> {
        let api_version = self.header.api_version;

        self.header.encode::<T>(dst)?;

        dst.put_i32::<T>(self.replica_id);
        dst.put_array::<T, _, _>(self.topics, |buf, topic| {
            buf.put_str::<T, _>(Some(topic.topic_name))?;
            buf.put_array::<T, _, _>(topic.partitions, |buf, partition| {
                buf.put_i32::<T>(partition.partition);
                buf.put_i64::<T>(partition.timestamp);
                if api_version == 0 {
                    buf.put_i32::<T>(partition.max_number_of_offsets);
                }
                Ok(())
            })
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListOffsetResponse {
    pub header: ResponseHeader,
    pub topics: Vec<TopicOffset>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopicOffset {
    /// The name of the topic.
    pub topic_name: String,
    pub partitions: Vec<PartitionOffset>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PartitionOffset {
    /// The id of the partition the fetch is for.
    pub partition: PartitionId,
    pub error_code: ErrorCode,
    pub timestamp: Option<Timestamp>,
    pub offsets: Vec<Offset>,
}

named_args!(pub parse_list_offset_response(api_version: ApiVersion)<ListOffsetResponse>,
    parse_tag!(ParseTag::ListOffsetResponse,
        do_parse!(
            header: parse_response_header
         >> topics: length_count!(be_i32, apply!(parse_list_topic_offset, api_version))
         >> (ListOffsetResponse {
                header: header,
                topics: topics,
            })
        )
    )
);

named_args!(parse_list_topic_offset(api_version: ApiVersion)<TopicOffset>,
    parse_tag!(ParseTag::TopicOffset,
        do_parse!(
            topic_name: parse_string
         >> partitions: length_count!(be_i32, apply!(parse_list_partition_offset, api_version))
         >> (TopicOffset {
                topic_name: topic_name,
                partitions: partitions,
            })
        )
    )
);

named_args!(parse_list_partition_offset(api_version: ApiVersion)<PartitionOffset>,
    parse_tag!(ParseTag::PartitionOffset,
        do_parse!(
            partition: be_i32
         >> error_code: be_i16
         >> offsets: cond!(api_version == 0, length_count!(be_i32, be_i64))
         >> timestamp: cond!(api_version > 0, be_i64)
         >> offset: cond!(api_version > 0, be_i64)
         >> (PartitionOffset {
                partition: partition,
                error_code: error_code,
                timestamp: timestamp,
                offsets: if api_version == 0 { offsets.unwrap_or_default() } else { vec![offset.unwrap_or_default()] },
            })
        )
    )
);

#[cfg(test)]
mod tests {

    use super::*;
    use bytes::BigEndian;

    use nom::IResult;
    use protocol::*;

    #[test]
    fn test_encode_list_offset_request_v0() {
        let req = ListOffsetRequest {
            header: RequestHeader {
                api_key: ApiKeys::ListOffsets as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            replica_id: 2,
            topics: vec![ListTopicOffset {
                topic_name: "topic".into(),
                partitions: vec![ListPartitionOffset {
                    partition: 5,
                    timestamp: 6,
                    max_number_of_offsets: 7
                }],
            }],
        };

        let data = vec![
            // FetchRequest
                // RequestHeader
                0, 2,                               // api_key
                0, 0,                               // api_version
                0, 0, 0, 123,                       // correlation_id
                0, 6, 99, 108, 105, 101, 110, 116,  // client_id
            0, 0, 0, 2,                             // replica_id
                // topics: [ListTopicOffset]
                0, 0, 0, 1,
                    // ListTopicOffset
                    0, 5, 116, 111, 112, 105, 99,   // topic_name
                    // partitions: [ListPartitionOffset]
                    0, 0, 0, 1,
                        // ListPartitionOffset
                        0, 0, 0, 5,                 // partition
                        0, 0, 0, 0, 0, 0, 0, 6,     // timestamp
                        0, 0, 0, 7,                 // max_number_of_offsets
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_encode_list_offset_request_v1() {
        let req = ListOffsetRequest {
            header: RequestHeader {
                api_key: ApiKeys::ListOffsets as ApiKey,
                api_version: 1,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            replica_id: 2,
            topics: vec![ListTopicOffset {
                topic_name: "topic".into(),
                partitions: vec![ListPartitionOffset {
                    partition: 5,
                    timestamp: 6,
                    max_number_of_offsets: 0
                }],
            }],
        };

        let data = vec![
            // FetchRequest
                // RequestHeader
                0, 2,                               // api_key
                0, 1,                               // api_version
                0, 0, 0, 123,                       // correlation_id
                0, 6, 99, 108, 105, 101, 110, 116,  // client_id
            0, 0, 0, 2,                             // replica_id
                // topics: [ListTopicOffset]
                0, 0, 0, 1,
                    // ListTopicOffset
                    0, 5, 116, 111, 112, 105, 99,   // topic_name
                    // partitions: [ListPartitionOffset]
                    0, 0, 0, 1,
                        // ListPartitionOffset
                        0, 0, 0, 5,                 // partition
                        0, 0, 0, 0, 0, 0, 0, 6,     // timestamp
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_list_offset_response_v0() {
        let response = ListOffsetResponse {
            header: ResponseHeader { correlation_id: 123 },
            topics: vec![TopicOffset {
                topic_name: "topic".to_owned(),
                partitions: vec![PartitionOffset {
                    partition: 1,
                    error_code: 2,
                    timestamp: None,
                    offsets: vec![3, 4, 5, 6],
                }],
            }],
        };

        let data = vec![
            // ResponseHeader
            0, 0, 0, 123,   // correlation_id
            // topics: [TopicData]
            0, 0, 0, 1,
                0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                // partitions: [PartitionData]
                0, 0, 0, 1,
                    0, 0, 0, 1,             // partition
                    0, 2,                   // error_code
                    // offsets: [Offset]
                    0, 0, 0, 4,
                        0, 0, 0, 0, 0, 0, 0, 3,
                        0, 0, 0, 0, 0, 0, 0, 4,
                        0, 0, 0, 0, 0, 0, 0, 5,
                        0, 0, 0, 0, 0, 0, 0, 6,
        ];

        let res = parse_list_offset_response(&data[..], 0);

        display_parse_error::<_>(&data[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], response));
    }

    #[test]
    fn test_parse_list_offset_response_v1() {
        let response = ListOffsetResponse {
            header: ResponseHeader { correlation_id: 123 },
            topics: vec![TopicOffset {
                topic_name: "topic".to_owned(),
                partitions: vec![PartitionOffset {
                    partition: 1,
                    error_code: 2,
                    timestamp: Some(3),
                    offsets: vec![4],
                }],
            }],
        };

        let data = vec![
            // ResponseHeader
            0, 0, 0, 123,   // correlation_id
            // topics: [TopicData]
            0, 0, 0, 1,
                0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                // partitions: [PartitionData]
                0, 0, 0, 1,
                    0, 0, 0, 1,             // partition
                    0, 2,                   // error_code
                    0, 0, 0, 0, 0, 0, 0, 3, // timestamp
                    0, 0, 0, 0, 0, 0, 0, 4, // offset
        ];

        let res = parse_list_offset_response(&data[..], 1);

        display_parse_error::<_>(&data[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], response));
    }
}
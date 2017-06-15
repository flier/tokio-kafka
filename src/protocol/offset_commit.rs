use bytes::{BufMut, ByteOrder, BytesMut};
use std::borrow::Cow;

use nom::{IResult, be_i16, be_i32};

use errors::Result;
use protocol::{ARRAY_LEN_SIZE, ApiVersion, Encodable, ErrorCode, OFFSET_SIZE, Offset,
               PARTITION_ID_SIZE, ParseTag, PartitionId, Record, RequestHeader, ResponseHeader,
               STR_LEN_SIZE, TIMESTAMP_SIZE, Timestamp, WriteExt, parse_response_header,
               parse_string};

const GROUP_GENERATION_ID_SIZE: usize = 4;
const RETENTION_TIME: usize = 8;

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetCommitRequest<'a> {
    pub header: RequestHeader<'a>,
    /// The group id.
    pub group_id: Cow<'a, str>,
    /// The generation of the group.
    pub group_generation_id: i32,
    /// The member id assigned by the group coordinator.
    pub member_id: Cow<'a, str>,
    /// Time period in ms to retain the offset.
    pub retention_time: i64,
    /// Topic to commit.
    pub topics: Vec<OffsetCommitTopic<'a>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetCommitTopic<'a> {
    /// The name of the topic.
    pub topic_name: Cow<'a, str>,
    /// Partitions to commit offset.
    pub partitions: Vec<OffsetCommitPartition<'a>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetCommitPartition<'a> {
    /// The id of the partition the commit is for.
    pub partition_id: PartitionId,
    /// Message offset to be committed.
    pub offset: Offset,
    /// Timestamp of the commit
    pub timestamp: Timestamp,
    /// Any associated metadata the client wants to keep.
    pub metadata: Option<Cow<'a, str>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetCommitResponse {
    pub header: ResponseHeader,
    /// Topics to commit offsets.
    pub topics: Vec<OffsetCommitTopicStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetCommitTopicStatus {
    /// The name of the topic.
    pub topic_name: String,
    /// Partitions to commit offset.
    pub partitions: Vec<OffsetCommitPartitionStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetCommitPartitionStatus {
    /// The id of the partition the commit is for.
    pub partition_id: PartitionId,
    /// Error code.
    pub error_code: ErrorCode,
}

impl<'a> Record for OffsetCommitRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + STR_LEN_SIZE + self.group_id.len() +
            if api_version > 0 {
                GROUP_GENERATION_ID_SIZE + STR_LEN_SIZE + self.member_id.len()
            } else {
                0
            } + if api_version > 1 { RETENTION_TIME } else { 0 } +
            self.topics.iter().fold(ARRAY_LEN_SIZE, |size, topic| {
                size + STR_LEN_SIZE + topic.topic_name.len() +
                    topic.partitions.iter().fold(
                        ARRAY_LEN_SIZE,
                        |size, partition| {
                            size + PARTITION_ID_SIZE + OFFSET_SIZE +
                                if api_version == 1 { TIMESTAMP_SIZE } else { 0 } +
                                STR_LEN_SIZE +
                                partition.metadata.as_ref().map_or(0, |s| s.len())
                        },
                    )
            })
    }
}

impl<'a> Encodable for OffsetCommitRequest<'a> {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        let api_version = self.header.api_version;

        self.header.encode::<T>(dst)?;

        dst.put_str::<T, _>(Some(self.group_id.as_ref()))?;
        if api_version > 0 {
            dst.put_i32::<T>(self.group_generation_id);
            dst.put_str::<T, _>(Some(self.member_id.as_ref()))?;
        }
        if api_version > 1 {
            dst.put_i64::<T>(self.retention_time);
        }
        dst.put_array::<T, _, _>(&self.topics, |buf, topic| {
            buf.put_str::<T, _>(Some(topic.topic_name.as_ref()))?;
            buf.put_array::<T, _, _>(&topic.partitions, |buf, partition| {
                buf.put_i32::<T>(partition.partition_id);
                buf.put_i64::<T>(partition.offset);
                if api_version == 1 {
                    buf.put_i64::<T>(partition.timestamp);
                }
                buf.put_str::<T, _>(partition.metadata.as_ref())
            })
        })
    }
}

impl OffsetCommitResponse {
    pub fn parse(buf: &[u8]) -> IResult<&[u8], Self> {
        parse_offset_commit_response(buf)
    }
}

named!(parse_offset_commit_response<OffsetCommitResponse>,
    parse_tag!(ParseTag::OffsetCommitResponse,
        do_parse!(
            header: parse_response_header
         >> topics: length_count!(be_i32, parse_offset_commit_topic_status)
         >> (OffsetCommitResponse {
                header: header,
                topics: topics,
            })
        )
    )
);

named!(parse_offset_commit_topic_status<OffsetCommitTopicStatus>,
    parse_tag!(ParseTag::OffsetCommitTopicStatus,
        do_parse!(
            topic_name: parse_string
         >> partitions: length_count!(be_i32, parse_offset_commit_partition_status)
         >> (OffsetCommitTopicStatus {
                topic_name: topic_name,
                partitions: partitions,
            })
        )
    )
);

named!(parse_offset_commit_partition_status<OffsetCommitPartitionStatus>,
    parse_tag!(ParseTag::OffsetCommitPartitionStatus,
        do_parse!(
            partition_id: be_i32
         >> error_code: be_i16
         >> (OffsetCommitPartitionStatus {
                partition_id: partition_id,
                error_code: error_code,
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
    fn test_encode_offset_commit_request_v0() {
        let req = OffsetCommitRequest {
            header: RequestHeader {
                api_key: ApiKeys::OffsetCommit as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            group_id: "consumer".into(),
            group_generation_id: Default::default(),
            member_id: "member".into(),
            retention_time: Default::default(),
            topics: vec![OffsetCommitTopic {
                topic_name: "topic".into(),
                partitions: vec![OffsetCommitPartition {
                    partition_id: 5,
                    offset: 6,
                    timestamp: Default::default(),
                    metadata: Some("metadata".into())
                }],
            }],
        };

        let data = vec![
            // OffsetCommitRequest
                // RequestHeader
                0, 8,                                       // api_key
                0, 0,                                       // api_version
                0, 0, 0, 123,                               // correlation_id
                0, 6, b'c', b'l', b'i', b'e', b'n', b't',   // client_id

            0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r',   // group_id

                // topics: [OffsetCommitTopic]
                0, 0, 0, 1,
                    // OffsetCommitTopic
                    0, 5, b't', b'o', b'p', b'i', b'c',             // topic_name
                    // partitions: [OffsetCommitPartition]
                    0, 0, 0, 1,
                        // OffsetCommitPartition
                        0, 0, 0, 5,                                             // partition
                        0, 0, 0, 0, 0, 0, 0, 6,                                 // offset
                        0, 8, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a',   // metadata
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_encode_offset_commit_request_v1() {
        let req = OffsetCommitRequest {
            header: RequestHeader {
                api_key: ApiKeys::OffsetCommit as ApiKey,
                api_version: 1,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            group_id: "consumer".into(),
            group_generation_id: 456,
            member_id: "member".into(),
            retention_time: Default::default(),
            topics: vec![OffsetCommitTopic {
                topic_name: "topic".into(),
                partitions: vec![OffsetCommitPartition {
                    partition_id: 5,
                    offset: 6,
                    timestamp: 7,
                    metadata: Some("metadata".into())
                }],
            }],
        };

        let data = vec![
            // OffsetCommitRequest
                // RequestHeader
                0, 8,                                       // api_key
                0, 1,                                       // api_version
                0, 0, 0, 123,                               // correlation_id
                0, 6, b'c', b'l', b'i', b'e', b'n', b't',   // client_id

            0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r',   // group_id
            0, 0, 1, 200,                                           // group_generation_id
            0, 6, b'm', b'e', b'm', b'b', b'e', b'r',               // member_id

                // topics: [OffsetCommitTopic]
                0, 0, 0, 1,
                    // OffsetCommitTopic
                    0, 5, b't', b'o', b'p', b'i', b'c',             // topic_name
                    // partitions: [OffsetCommitPartition]
                    0, 0, 0, 1,
                        // OffsetCommitPartition
                        0, 0, 0, 5,                                             // partition
                        0, 0, 0, 0, 0, 0, 0, 6,                                 // offset
                        0, 0, 0, 0, 0, 0, 0, 7,                                 // timestamp
                        0, 8, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a',   // metadata
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_encode_offset_commit_request_v2() {
        let req = OffsetCommitRequest {
            header: RequestHeader {
                api_key: ApiKeys::OffsetCommit as ApiKey,
                api_version: 2,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            group_id: "consumer".into(),
            group_generation_id: 456,
            member_id: "member".into(),
            retention_time: 789,
            topics: vec![OffsetCommitTopic {
                topic_name: "topic".into(),
                partitions: vec![OffsetCommitPartition {
                    partition_id: 5,
                    offset: 6,
                    timestamp: Default::default(),
                    metadata: Some("metadata".into())
                }],
            }],
        };

        let data = vec![
            // OffsetCommitRequest
                // RequestHeader
                0, 8,                                       // api_key
                0, 2,                                       // api_version
                0, 0, 0, 123,                               // correlation_id
                0, 6, b'c', b'l', b'i', b'e', b'n', b't',   // client_id

            0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r',   // group_id
            0, 0, 1, 200,                                           // group_generation_id
            0, 6, b'm', b'e', b'm', b'b', b'e', b'r',               // member_id
            0, 0, 0, 0, 0, 0, 3, 21,                                // retention_time

                // topics: [OffsetCommitTopic]
                0, 0, 0, 1,
                    // OffsetCommitTopic
                    0, 5, b't', b'o', b'p', b'i', b'c',             // topic_name
                    // partitions: [OffsetCommitPartition]
                    0, 0, 0, 1,
                        // OffsetCommitPartition
                        0, 0, 0, 5,                                             // partition
                        0, 0, 0, 0, 0, 0, 0, 6,                                 // offset
                        0, 8, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a',   // metadata
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_offset_commit_response() {
        let response = OffsetCommitResponse {
            header: ResponseHeader { correlation_id: 123 },
            topics: vec![OffsetCommitTopicStatus {
                topic_name: "topic".to_owned(),
                partitions: vec![OffsetCommitPartitionStatus {
                    partition_id: 1,
                    error_code: 2,
                }],
            }],
        };

        let data = vec![
            // ResponseHeader
            0, 0, 0, 123,   // correlation_id
            // topics: [OffsetCommitTopicStatus]
            0, 0, 0, 1,
                0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                // partitions: [OffsetCommitPartitionStatus]
                0, 0, 0, 1,
                    0, 0, 0, 1,             // partition
                    0, 2,                   // error_code
        ];

        let res = parse_offset_commit_response(&data[..]);

        display_parse_error::<_>(&data[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], response));
    }
}

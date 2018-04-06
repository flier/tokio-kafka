use std::borrow::Cow;
use std::i32;

use bytes::{BufMut, ByteOrder, BytesMut};

use nom::{IResult, be_i16, be_i32, be_i64};

use errors::Result;
use protocol::{parse_message_set, parse_response_header, parse_string, ApiVersion, Encodable, ErrorCode, MessageSet,
               Offset, ParseTag, PartitionId, Record, ReplicaId, RequestHeader, ResponseHeader, WriteExt,
               ARRAY_LEN_SIZE, OFFSET_SIZE, PARTITION_ID_SIZE, REPLICA_ID_SIZE, STR_LEN_SIZE};

pub const DEFAULT_RESPONSE_MAX_BYTES: i32 = i32::MAX;

const MAX_WAIT_TIME: usize = 4;
const MIN_BYTES_SIZE: usize = 4;
const MAX_BYTES_SIZE: usize = 4;
const REQUEST_OVERHEAD: usize = REPLICA_ID_SIZE + MAX_WAIT_TIME + MIN_BYTES_SIZE;
const FETCH_OFFSET_SIZE: usize = OFFSET_SIZE;

#[derive(Clone, Debug, PartialEq)]
pub struct FetchRequest<'a> {
    pub header: RequestHeader<'a>,
    /// The replica id indicates the node id of the replica initiating this request.
    pub replica_id: ReplicaId,
    /// The maximum amount of time in milliseconds to block waiting if insufficient data is
    /// available at the time the request is issued.
    pub max_wait_time: i32,
    /// This is the minimum number of bytes of messages that must be available to give a
    /// response.
    pub min_bytes: i32,
    /// Maximum bytes to accumulate in the response.
    ///
    /// Note that this is not an absolute maximum, if the first message in the first non-empty
    /// partition of
    /// the fetch is larger than this value, the message will still be returned to ensure that
    /// progress can be made.
    pub max_bytes: i32,
    /// Topics to fetch in the order provided.
    pub topics: Vec<FetchTopic<'a>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchTopic<'a> {
    /// The name of the topic.
    pub topic_name: Cow<'a, str>,
    /// Partitions to fetch.
    pub partitions: Vec<FetchPartition>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchPartition {
    /// The id of the partition the fetch is for.
    pub partition_id: PartitionId,
    /// The offset to begin this fetch from.
    pub fetch_offset: Offset,
    /// The maximum bytes to include in the message set for this partition.
    pub max_bytes: i32,
}

impl<'a> Record for FetchRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + REQUEST_OVERHEAD + if api_version > 2 { MAX_BYTES_SIZE } else { 0 }
            + self.topics.iter().fold(ARRAY_LEN_SIZE, |size, topic| {
                size + STR_LEN_SIZE + topic.topic_name.len()
                    + topic.partitions.iter().fold(ARRAY_LEN_SIZE, |size, _| {
                        size + PARTITION_ID_SIZE + FETCH_OFFSET_SIZE + MAX_BYTES_SIZE
                    })
            })
    }
}

impl<'a> Encodable for FetchRequest<'a> {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        let api_version = self.header.api_version;

        self.header.encode::<T>(dst)?;

        dst.put_i32::<T>(self.replica_id);
        dst.put_i32::<T>(self.max_wait_time);
        dst.put_i32::<T>(self.min_bytes);
        if api_version > 2 {
            dst.put_i32::<T>(self.max_bytes);
        }
        dst.put_array::<T, _, _>(&self.topics, |buf, topic| {
            buf.put_str::<T, _>(Some(topic.topic_name.as_ref()))?;
            buf.put_array::<T, _, _>(&topic.partitions, |buf, partition| {
                buf.put_i32::<T>(partition.partition_id);
                buf.put_i64::<T>(partition.fetch_offset);
                buf.put_i32::<T>(partition.max_bytes);
                Ok(())
            })
        })?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchResponse {
    pub header: ResponseHeader,
    /// Duration in milliseconds for which the request was throttled due to quota violation.
    pub throttle_time: Option<i32>,
    pub topics: Vec<FetchTopicData>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchTopicData {
    /// The name of the topic this response entry is for.
    pub topic_name: String,
    pub partitions: Vec<FetchPartitionData>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchPartitionData {
    /// The id of the partition the fetch is for.
    pub partition_id: PartitionId,
    pub error_code: ErrorCode,
    /// The offset at the end of the log for this partition.
    pub high_watermark: Offset,
    pub message_set: MessageSet,
}

impl FetchResponse {
    pub fn parse(buf: &[u8], api_version: ApiVersion) -> IResult<&[u8], Self> {
        parse_fetch_response(buf, api_version)
    }
}

named_args!(parse_fetch_response(api_version: ApiVersion)<FetchResponse>,
    parse_tag!(ParseTag::FetchResponse,
        do_parse!(
            header: parse_response_header
         >> throttle_time: cond!(api_version > 0, be_i32)
         >> topics: length_count!(be_i32, apply!(parse_fetch_topic_data, api_version))
         >> (FetchResponse {
                header: header,
                throttle_time: throttle_time,
                topics: topics,
            })
        )
    )
);

named_args!(parse_fetch_topic_data(api_version: ApiVersion)<FetchTopicData>,
    parse_tag!(ParseTag::FetchTopicData,
        do_parse!(
            topic_name: parse_string
         >> partitions: length_count!(be_i32, apply!(parse_fetch_partition_data, api_version))
         >> (FetchTopicData {
                topic_name: topic_name,
                partitions: partitions,
            })
        )
    )
);

named_args!(parse_fetch_partition_data(api_version: ApiVersion)<FetchPartitionData>,
    parse_tag!(ParseTag::FetchPartitionData,
        do_parse!(
            partition_id: be_i32
         >> error_code: be_i16
         >> high_watermark: be_i64
         >> message_set: length_value!(be_i32, apply!(parse_message_set, api_version))
         >> (FetchPartitionData {
                partition_id: partition_id,
                error_code: error_code,
                high_watermark: high_watermark,
                message_set: message_set,
            })
        )
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BigEndian, Bytes};
    use compression::Compression;

    use nom::IResult;
    use protocol::*;

    #[test]
    fn encode_fetch_request() {
        let request = FetchRequest {
            header: RequestHeader {
                api_key: ApiKeys::Fetch as ApiKey,
                api_version: 1,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            replica_id: 2,
            max_wait_time: 3,
            min_bytes: 4,
            max_bytes: 0,
            topics: vec![
                FetchTopic {
                    topic_name: "topic".into(),
                    partitions: vec![
                        FetchPartition {
                            partition_id: 5,
                            fetch_offset: 6,
                            max_bytes: 7,
                        },
                    ],
                },
            ],
        };

        let data = vec![
            /* FetchRequest
             * RequestHeader */ 0, 1 /* api_key */, 0, 1 /* api_version */,
            0, 0, 0, 123 /* correlation_id */, 0, 6, 99, 108, 105, 101, 110, 116 /* client_id */, 0, 0, 0,
            2 /* replica_id */, 0, 0, 0, 3 /* max_wait_time */, 0, 0, 0, 4 /* max_bytes */,
            /* topics: [FetchTopicData] */ 0, 0, 0, 1, /* FetchTopicData */ 0, 5, 116, 111, 112, 105,
            99 /* topic_name */, /* partitions: [FetchPartitionData] */ 0, 0, 0, 1,
            /* FetchPartitionData */ 0, 0, 0, 5 /* partition */, 0, 0, 0, 0, 0, 0, 0,
            6 /* fetch_offset */, 0, 0, 0, 7 /* max_bytes */,
        ];

        let mut buf = BytesMut::with_capacity(128);

        request.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(request.size(request.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn encode_fetch_request_v3() {
        let request = FetchRequest {
            header: RequestHeader {
                api_key: ApiKeys::Fetch as ApiKey,
                api_version: 3,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            replica_id: 2,
            max_wait_time: 3,
            min_bytes: 4,
            max_bytes: 1024,
            topics: vec![
                FetchTopic {
                    topic_name: "topic".into(),
                    partitions: vec![
                        FetchPartition {
                            partition_id: 5,
                            fetch_offset: 6,
                            max_bytes: 7,
                        },
                    ],
                },
            ],
        };

        let data = vec![
            /* FetchRequest
             * RequestHeader */ 0, 1 /* api_key */, 0, 3 /* api_version */,
            0, 0, 0, 123 /* correlation_id */, 0, 6, 99, 108, 105, 101, 110, 116 /* client_id */, 0, 0, 0,
            2 /* replica_id */, 0, 0, 0, 3 /* max_wait_time */, 0, 0, 0, 4 /* min_bytes */, 0, 0, 4,
            0 /* max_bytes */, /* topics: [FetchTopicData] */ 0, 0, 0, 1, /* FetchTopicData */ 0, 5,
            116, 111, 112, 105, 99 /* topic_name */, /* partitions: [FetchPartitionData] */ 0, 0, 0, 1,
            /* FetchPartitionData */ 0, 0, 0, 5 /* partition */, 0, 0, 0, 0, 0, 0, 0,
            6 /* fetch_offset */, 0, 0, 0, 7 /* max_bytes */,
        ];

        let mut buf = BytesMut::with_capacity(128);

        request.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(request.size(request.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn parse_fetch_response_v0() {
        let response = FetchResponse {
            header: ResponseHeader { correlation_id: 123 },
            throttle_time: None,
            topics: vec![
                FetchTopicData {
                    topic_name: "topic".to_owned(),
                    partitions: vec![
                        FetchPartitionData {
                            partition_id: 1,
                            error_code: 2,
                            high_watermark: 3,
                            message_set: MessageSet {
                                messages: vec![
                                    Message {
                                        offset: 0,
                                        compression: Compression::None,
                                        key: Some(Bytes::from(&b"key"[..])),
                                        value: Some(Bytes::from(&b"value"[..])),
                                        timestamp: None,
                                    },
                                ],
                            },
                        },
                    ],
                },
            ],
        };

        let data = vec![
            /* ResponseHeader */ 0, 0, 0, 123 /* correlation_id */,
            /* 0, 0, 0, 1,     // throttle_time
             * topics: [TopicData] */ 0, 0, 0, 1, 0, 5, b't',
            b'o', b'p', b'i', b'c' /* topic_name */, /* partitions: [PartitionData] */ 0, 0, 0, 1, 0, 0, 0,
            1 /* partition */, 0, 2 /* error_code */, 0, 0, 0, 0, 0, 0, 0,
            3 /* highwater_mark_offset */, /* MessageSet */ 0, 0, 0, 34 /* size */,
            /* messages: [Message] */ 0, 0, 0, 0, 0, 0, 0, 0 /* offset */, 0, 0, 0, 22 /* size */, 197,
            70, 142, 169 /* crc */, 0 /* magic */, 8 /* attributes */,
            /* 0, 0, 0, 0, 0, 0, 1, 200,           // timestamp */ 0, 0, 0, 3, 107, 101, 121 /* key */, 0, 0,
            0, 5, 118, 97, 108, 117, 101 /* value */,
        ];

        let res = parse_fetch_response(&data[..], 0);

        display_parse_error::<_>(&data[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], response));
    }

    #[test]
    fn parse_fetch_response_v1() {
        let response = FetchResponse {
            header: ResponseHeader { correlation_id: 123 },
            throttle_time: Some(1),
            topics: vec![
                FetchTopicData {
                    topic_name: "topic".to_owned(),
                    partitions: vec![
                        FetchPartitionData {
                            partition_id: 1,
                            error_code: 2,
                            high_watermark: 3,
                            message_set: MessageSet {
                                messages: vec![
                                    Message {
                                        offset: 0,
                                        compression: Compression::None,
                                        key: Some(Bytes::from(&b"key"[..])),
                                        value: Some(Bytes::from(&b"value"[..])),
                                        timestamp: Some(MessageTimestamp::LogAppendTime(456)),
                                    },
                                ],
                            },
                        },
                    ],
                },
            ],
        };

        let data = vec![
            /* ResponseHeader */ 0, 0, 0, 123 /* correlation_id */, 0, 0, 0, 1 /* throttle_time */,
            /* topics: [TopicData] */ 0, 0, 0, 1, 0, 5, b't', b'o', b'p', b'i', b'c' /* topic_name */,
            /* partitions: [PartitionData] */ 0, 0, 0, 1, 0, 0, 0, 1 /* partition */, 0,
            2 /* error_code */, 0, 0, 0, 0, 0, 0, 0, 3 /* highwater_mark_offset */, /* MessageSet */ 0,
            0, 0, 42 /* size */, /* messages: [Message] */ 0, 0, 0, 0, 0, 0, 0, 0 /* offset */, 0, 0, 0,
            30 /* size */, 206, 63, 210, 11 /* crc */, 1 /* magic */, 8 /* attributes */, 0, 0, 0,
            0, 0, 0, 1, 200 /* timestamp */, 0, 0, 0, 3, 107, 101, 121 /* key */, 0, 0, 0, 5, 118, 97, 108,
            117, 101 /* value */,
        ];

        let res = parse_fetch_response(&data[..], 1);

        display_parse_error::<_>(&data[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], response));
    }
}

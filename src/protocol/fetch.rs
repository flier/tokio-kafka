use std::i32;
use std::borrow::Cow;
use std::collections::HashMap;

use bytes::{BufMut, ByteOrder, BytesMut};

use nom::{IResult, be_i16, be_i32, be_i64};

use errors::Result;
use protocol::{parse_message_set, parse_response_header, parse_string, ApiVersion, Encodable, ErrorCode, MessageSet,
               Offset, ParseTag, PartitionId, ProducerId, ReplicaId, Request, RequestHeader, ResponseHeader,
               SessionId, WriteExt, ARRAY_LEN_SIZE, OFFSET_SIZE, PARTITION_ID_SIZE, REPLICA_ID_SIZE, STR_LEN_SIZE};
use network::TopicPartition;

pub const DEFAULT_RESPONSE_MAX_BYTES: i32 = i32::MAX;

const MAX_WAIT_TIME: usize = 4;
const MIN_BYTES_SIZE: usize = 4;
const MAX_BYTES_SIZE: usize = 4;
const ISOLATION_LEVEL_SIZE: usize = 1;
const SESSION_ID_SIZE: usize = 4;
const SESSION_EPOCH_SIZE: usize = 4;
const REQUEST_OVERHEAD: usize = REPLICA_ID_SIZE + MAX_WAIT_TIME + MIN_BYTES_SIZE;
const FETCH_OFFSET_SIZE: usize = OFFSET_SIZE;
const LOG_START_OFFSET_SIZE: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum IsolationLevel {
    /// makes all records visible.
    #[serde(rename = "read_uncommitted")]
    ReadUncommitted = 0,
    /// non-transactional and COMMITTED transactional records are visible.
    #[serde(rename = "read_committed")]
    ReadCommitted = 1,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::ReadUncommitted
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchSession<'a> {
    /// The fetch session ID
    pub id: SessionId,
    /// The fetch epoch
    pub epoch: i32,
    /// Topics to remove from the fetch session.
    pub forgetten_topics: Vec<TopicPartition<'a>>,
}

impl<'a> FetchSession<'a> {
    fn forgetten_topics(&self) -> HashMap<Cow<'a, str>, Vec<PartitionId>> {
        self.forgetten_topics.iter().fold(HashMap::new(), |mut topics, ref tp| {
            topics
                .entry(tp.topic_name.clone())
                .or_insert_with(|| Vec::new())
                .push(tp.partition_id);
            topics
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchRequest<'a> {
    pub header: RequestHeader<'a>,
    /// The replica id indicates the node id of the replica initiating this
    /// request.
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
    /// This setting controls the visibility of transactional records.
    pub isolation_level: IsolationLevel,
    /// Topics to fetch in the order provided.
    pub topics: Vec<FetchTopic<'a>>,
    /// The fetch session
    pub session: Option<FetchSession<'a>>,
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
    /// Earliest available offset of the follower replica.
    ///
    /// The field is only used when request is sent by follower.
    pub log_start_offset: Option<Offset>,
    /// The maximum bytes to include in the message set for this partition.
    pub max_bytes: i32,
}

impl<'a> Request for FetchRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + REQUEST_OVERHEAD + if api_version > 2 { MAX_BYTES_SIZE } else { 0 }
            + if api_version > 3 { ISOLATION_LEVEL_SIZE } else { 0 }
            + self.topics.iter().fold(ARRAY_LEN_SIZE, |size, topic| {
                size + STR_LEN_SIZE + topic.topic_name.len()
                    + topic.partitions.iter().fold(ARRAY_LEN_SIZE, |size, _| {
                        size + PARTITION_ID_SIZE + FETCH_OFFSET_SIZE + if api_version > 4 {
                            LOG_START_OFFSET_SIZE
                        } else {
                            0
                        } + MAX_BYTES_SIZE
                    })
            }) + if api_version > 6 {
            SESSION_ID_SIZE + SESSION_EPOCH_SIZE + self.session.as_ref().map_or(ARRAY_LEN_SIZE, |session| {
                ARRAY_LEN_SIZE
                    + session
                        .forgetten_topics()
                        .into_iter()
                        .map(|(topic_name, partitions)| {
                            STR_LEN_SIZE + topic_name.len() + ARRAY_LEN_SIZE + PARTITION_ID_SIZE * partitions.len()
                        })
                        .sum::<usize>()
            })
        } else {
            0
        }
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
        if api_version > 3 {
            dst.put_u8(self.isolation_level as u8);
        }
        if api_version > 6 {
            if let Some(FetchSession { id, epoch, .. }) = self.session {
                dst.put_i32::<T>(id);
                dst.put_i32::<T>(epoch);
            }
        }
        dst.put_array::<T, _, _>(&self.topics, |buf, topic| {
            buf.put_str::<T, _>(Some(topic.topic_name.as_ref()))?;
            buf.put_array::<T, _, _>(&topic.partitions, |buf, partition| {
                buf.put_i32::<T>(partition.partition_id);
                buf.put_i64::<T>(partition.fetch_offset);
                if api_version > 4 {
                    buf.put_i64::<T>(partition.log_start_offset.unwrap_or_default());
                }
                buf.put_i32::<T>(partition.max_bytes);
                Ok(())
            })
        })?;
        if api_version > 6 {
            if let Some(ref session) = self.session {
                let topics = session.forgetten_topics().into_iter().collect::<Vec<_>>();

                dst.put_array::<T, _, _>(&topics, |buf, &(ref topic_name, ref partitions)| {
                    buf.put_str::<T, _>(Some(topic_name))?;
                    buf.put_array::<T, _, _>(&partitions, |buf, &partition_id| {
                        buf.put_i32::<T>(partition_id);
                        Ok(())
                    })
                })?;
            } else {
                dst.put_i32::<T>(0);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchResponse {
    pub header: ResponseHeader,
    /// Duration in milliseconds for which the request was throttled due to
    /// quota violation.
    pub throttle_time: Option<i32>,
    /// Response error code
    pub error_code: Option<ErrorCode>,
    /// The fetch session ID
    pub session_id: Option<SessionId>,
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
    /// Response error code
    pub error_code: ErrorCode,
    /// The offset at the end of the log for this partition.
    pub high_watermark: Offset,
    /// The last stable offset (or LSO) of the partition.
    ///
    /// This is the last offset such that the state of all transactional records prior to this offset have been
    /// decided (ABORTED or COMMITTED)
    pub last_stable_offset: Option<Offset>,
    /// Earliest available offset.
    pub log_start_offset: Option<Offset>,
    pub aborted_transactions: Option<FetchAbortedTransactions>,
    pub message_set: MessageSet,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchAbortedTransactions {
    /// The producer id associated with the aborted transactions
    pub producer_id: ProducerId,
    /// The first offset in the aborted transaction
    pub first_offset: Offset,
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
         >> error_code: cond!(api_version > 6, be_i16)
         >> session_id: cond!(api_version > 6, be_i32)
         >> topics: length_count!(be_i32, apply!(parse_fetch_topic_data, api_version))
         >> (FetchResponse {
                header,
                throttle_time,
                error_code,
                session_id,
                topics,
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
                topic_name,
                partitions,
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
         >> last_stable_offset: cond!(api_version > 3, be_i64)
         >> log_start_offset: cond!(api_version > 4, be_i64)
         >> aborted_transactions: cond!(api_version > 3, parse_aborted_transactions)
         >> message_set: length_value!(be_i32, parse_message_set)
         >> (FetchPartitionData {
                partition_id,
                error_code,
                high_watermark,
                last_stable_offset,
                log_start_offset,
                aborted_transactions,
                message_set,
            })
        )
    )
);

named!(
    parse_aborted_transactions<FetchAbortedTransactions>,
    do_parse!(
        producer_id: be_i64 >> first_offset: be_i64 >> (FetchAbortedTransactions {
            producer_id,
            first_offset,
        })
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BigEndian, Bytes};
    use compression::Compression;

    use nom::IResult;
    use protocol::*;

    lazy_static! {
        #[cfg_attr(rustfmt, rustfmt_skip)]
        static ref TEST_REQUESTS: Vec<(FetchRequest<'static>, &'static [u8])> = vec![
            (
                FetchRequest {
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
                    isolation_level: IsolationLevel::default(),
                    topics: vec![
                        FetchTopic {
                            topic_name: "topic".into(),
                            partitions: vec![
                                FetchPartition {
                                    partition_id: 5,
                                    fetch_offset: 6,
                                    log_start_offset: None,
                                    max_bytes: 7,
                                },
                            ],
                        },
                    ],
                    session: None,
                }, &[
                    // RequestHeader
                    0, 1,           // api_key
                    0, 1,           // api_version
                    0, 0, 0, 123,   // correlation_id
                    0, 6, b'c', b'l', b'i', b'e', b'n', b't', // client_id
                    // FetchRequest
                    0, 0, 0, 2,         // replica_id
                    0, 0, 0, 3,         // max_wait_time
                    0, 0, 0, 4,         // min_bytes
                    // topics: [FetchTopicData]
                    0, 0, 0, 1,
                        // FetchTopicData
                        0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                    // partitions: [FetchPartitionData]
                    0, 0, 0, 1,
                        // FetchPartitionData
                        0, 0, 0, 5,             // partition
                        0, 0, 0, 0, 0, 0, 0, 6, // fetch_offset
                        0, 0, 0, 7,             // max_bytes
                ][..]
            ), (
                FetchRequest {
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
                    isolation_level: IsolationLevel::default(),
                    topics: vec![
                        FetchTopic {
                            topic_name: "topic".into(),
                            partitions: vec![
                                FetchPartition {
                                    partition_id: 5,
                                    fetch_offset: 6,
                                    log_start_offset: None,
                                    max_bytes: 7,
                                },
                            ],
                        },
                    ],
                    session: None,
                }, &[
                    // RequestHeader
                    0, 1,           // api_key
                    0, 3,           // api_version
                    0, 0, 0, 123,   // correlation_id
                    0, 6, b'c', b'l', b'i', b'e', b'n', b't', // client_id
                    // FetchRequest
                    0, 0, 0, 2,         // replica_id
                    0, 0, 0, 3,         // max_wait_time
                    0, 0, 0, 4,         // min_bytes
                    0, 0, 4, 0,         // max_bytes
                    // topics: [FetchTopicData]
                    0, 0, 0, 1,
                        // FetchTopicData
                        0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                    // partitions: [FetchPartitionData]
                    0, 0, 0, 1,
                        // FetchPartitionData
                        0, 0, 0, 5,             // partition
                        0, 0, 0, 0, 0, 0, 0, 6, // fetch_offset
                        0, 0, 0, 7,             // max_bytes
                ][..]
            ), (
                FetchRequest {
                    header: RequestHeader {
                        api_key: ApiKeys::Fetch as ApiKey,
                        api_version: 4,
                        correlation_id: 123,
                        client_id: Some("client".into()),
                    },
                    replica_id: 2,
                    max_wait_time: 3,
                    min_bytes: 4,
                    max_bytes: 1024,
                    isolation_level: IsolationLevel::ReadCommitted,
                    topics: vec![
                        FetchTopic {
                            topic_name: "topic".into(),
                            partitions: vec![
                                FetchPartition {
                                    partition_id: 5,
                                    fetch_offset: 6,
                                    log_start_offset: None,
                                    max_bytes: 7,
                                },
                            ],
                        },
                    ],
                    session: None,
                }, &[
                    // RequestHeader
                    0, 1,           // api_key
                    0, 4,           // api_version
                    0, 0, 0, 123,   // correlation_id
                    0, 6, b'c', b'l', b'i', b'e', b'n', b't', // client_id
                    // FetchRequest
                    0, 0, 0, 2,         // replica_id
                    0, 0, 0, 3,         // max_wait_time
                    0, 0, 0, 4,         // min_bytes
                    0, 0, 4, 0,         // max_bytes
                    1,                  // isolation_level
                    // topics: [FetchTopicData]
                    0, 0, 0, 1,
                        // FetchTopicData
                        0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                    // partitions: [FetchPartitionData]
                    0, 0, 0, 1,
                        // FetchPartitionData
                        0, 0, 0, 5,             // partition
                        0, 0, 0, 0, 0, 0, 0, 6, // fetch_offset
                        0, 0, 0, 7,             // max_bytes
                ][..]
            ), (
                FetchRequest {
                    header: RequestHeader {
                        api_key: ApiKeys::Fetch as ApiKey,
                        api_version: 5,
                        correlation_id: 123,
                        client_id: Some("client".into()),
                    },
                    replica_id: 2,
                    max_wait_time: 3,
                    min_bytes: 4,
                    max_bytes: 1024,
                    isolation_level: IsolationLevel::ReadCommitted,
                    topics: vec![
                        FetchTopic {
                            topic_name: "topic".into(),
                            partitions: vec![
                                FetchPartition {
                                    partition_id: 5,
                                    fetch_offset: 6,
                                    log_start_offset: Some(8),
                                    max_bytes: 7,
                                },
                            ],
                        },
                    ],
                    session: None,
                }, &[
                    // RequestHeader
                    0, 1,           // api_key
                    0, 5,           // api_version
                    0, 0, 0, 123,   // correlation_id
                    0, 6, b'c', b'l', b'i', b'e', b'n', b't', // client_id
                    // FetchRequest
                    0, 0, 0, 2,         // replica_id
                    0, 0, 0, 3,         // max_wait_time
                    0, 0, 0, 4,         // min_bytes
                    0, 0, 4, 0,         // max_bytes
                    1,                  // isolation_level
                    // topics: [FetchTopicData]
                    0, 0, 0, 1,
                        // FetchTopicData
                        0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                    // partitions: [FetchPartitionData]
                    0, 0, 0, 1,
                        // FetchPartitionData
                        0, 0, 0, 5,             // partition
                        0, 0, 0, 0, 0, 0, 0, 6, // fetch_offset
                        0, 0, 0, 0, 0, 0, 0, 8, // log_start_offset
                        0, 0, 0, 7,             // max_bytes
                ][..]
            ), (
                FetchRequest {
                    header: RequestHeader {
                        api_key: ApiKeys::Fetch as ApiKey,
                        api_version: 7,
                        correlation_id: 123,
                        client_id: Some("client".into()),
                    },
                    replica_id: 2,
                    max_wait_time: 3,
                    min_bytes: 4,
                    max_bytes: 1024,
                    isolation_level: IsolationLevel::ReadCommitted,
                    topics: vec![
                        FetchTopic {
                            topic_name: "topic".into(),
                            partitions: vec![
                                FetchPartition {
                                    partition_id: 5,
                                    fetch_offset: 6,
                                    log_start_offset: Some(8),
                                    max_bytes: 7,
                                },
                            ],
                        },
                    ],
                    session: Some(FetchSession {
                        id: 9,
                        epoch: 10,
                        forgetten_topics: vec![
                            topic_partition!("topic", 0),
                            topic_partition!("topic", 1),
                        ],
                    }),
                }, &[
                    // RequestHeader
                    0, 1,           // api_key
                    0, 7,           // api_version
                    0, 0, 0, 123,   // correlation_id
                    0, 6, b'c', b'l', b'i', b'e', b'n', b't', // client_id
                    // FetchRequest
                    0, 0, 0, 2,         // replica_id
                    0, 0, 0, 3,         // max_wait_time
                    0, 0, 0, 4,         // min_bytes
                    0, 0, 4, 0,         // max_bytes
                    1,                  // isolation_level
                    0, 0, 0, 9,         // session id
                    0, 0, 0, 10,        // session epoch
                    // topics: [FetchTopicData]
                    0, 0, 0, 1,
                        // FetchTopicData
                        0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                    // partitions: [FetchPartitionData]
                    0, 0, 0, 1,
                        // FetchPartitionData
                        0, 0, 0, 5,             // partition
                        0, 0, 0, 0, 0, 0, 0, 6, // fetch_offset
                        0, 0, 0, 0, 0, 0, 0, 8, // log_start_offset
                        0, 0, 0, 7,             // max_bytes
                    // forgetten_topics
                    0, 0, 0, 1,
                        0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                        // partitions
                        0, 0, 0, 2,
                            0, 0, 0, 0,
                            0, 0, 0, 1,
                ][..]
            )
        ];
    }

    #[test]
    fn test_encode_fetch_request() {
        for &(ref request, encoded) in &*TEST_REQUESTS {
            let mut buf = BytesMut::with_capacity(128);

            request.encode::<BigEndian>(&mut buf).unwrap();

            assert_eq!(request.size(request.header.api_version), buf.len());

            assert_eq!(&buf[..], encoded);
        }
    }

    lazy_static! {
        #[cfg_attr(rustfmt, rustfmt_skip)]
        static ref TEST_RESPONSES: Vec<(ApiVersion, FetchResponse, &'static [u8])> = vec![
            (
                0,
                FetchResponse {
                    header: ResponseHeader { correlation_id: 123 },
                    throttle_time: None,
                    error_code: None,
                    session_id: None,
                    topics: vec![
                        FetchTopicData {
                            topic_name: "topic".to_owned(),
                            partitions: vec![
                                FetchPartitionData {
                                    partition_id: 1,
                                    error_code: 2,
                                    high_watermark: 3,
                                    last_stable_offset: None,
                                    log_start_offset: None,
                                    aborted_transactions: None,
                                    message_set: MessageSet {
                                        messages: vec![
                                            Message {
                                                offset: 0,
                                                compression: Compression::None,
                                                key: Some(Bytes::from(&b"key"[..])),
                                                value: Some(Bytes::from(&b"value"[..])),
                                                timestamp: None,
                                                headers: Vec::new(),
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    ],
                }, &[
                    // ResponseHeader
                    0, 0, 0, 123,   // correlation_id
                    // FetchResponse
                    // topics: [TopicData]
                    0, 0, 0, 1,
                        0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                        // partitions: [PartitionData]
                        0, 0, 0, 1,
                            0, 0, 0, 1,             // partition
                            0, 2,                   // error_code
                            0, 0, 0, 0, 0, 0, 0, 3, // highwater_mark
                            // MessageSet
                            0, 0, 0, 34,            // size
                                // messages: [Message]
                                0, 0, 0, 0, 0, 0, 0, 0,     // offset
                                0, 0, 0, 22,                // size
                                197, 70, 142, 169,          // crc
                                0,                          // magic
                                8,                          // attributes
                                0, 0, 0, 3, b'k', b'e', b'y',
                                0, 0, 0, 5, b'v', b'a', b'l', b'u', b'e',
                ]
            ), (
                1,
                FetchResponse {
                    header: ResponseHeader { correlation_id: 123 },
                    throttle_time: Some(1),
                    error_code: None,
                    session_id: None,
                    topics: vec![
                        FetchTopicData {
                            topic_name: "topic".to_owned(),
                            partitions: vec![
                                FetchPartitionData {
                                    partition_id: 1,
                                    error_code: 2,
                                    high_watermark: 3,
                                    last_stable_offset: None,
                                    log_start_offset: None,
                                    aborted_transactions: None,
                                    message_set: MessageSet {
                                        messages: vec![
                                            Message {
                                                offset: 0,
                                                compression: Compression::None,
                                                key: Some(Bytes::from(&b"key"[..])),
                                                value: Some(Bytes::from(&b"value"[..])),
                                                timestamp: Some(MessageTimestamp::LogAppendTime(456)),
                                                headers: Vec::new(),
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    ],
                }, &[
                    // ResponseHeader
                    0, 0, 0, 123,   // correlation_id
                    // FetchResponse
                    0, 0, 0, 1,     // throttle_time
                    // topics: [TopicData]
                    0, 0, 0, 1,
                        0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                        // partitions: [PartitionData]
                        0, 0, 0, 1,
                            0, 0, 0, 1,             // partition
                            0, 2,                   // error_code
                            0, 0, 0, 0, 0, 0, 0, 3, // highwater_mark
                            // MessageSet
                            0, 0, 0, 42,            // size
                                // messages: [Message]
                                0, 0, 0, 0, 0, 0, 0, 0,     // offset
                                0, 0, 0, 30,                // size
                                206, 63, 210, 11,           // crc
                                1,                          // magic
                                8,                          // attributes
                                0, 0, 0, 0, 0, 0, 1, 200,   // timestamp
                                0, 0, 0, 3, b'k', b'e', b'y',
                                0, 0, 0, 5, b'v', b'a', b'l', b'u', b'e',
                ]
            ), (
                4,
                FetchResponse {
                    header: ResponseHeader { correlation_id: 123 },
                    throttle_time: Some(1),
                    error_code: None,
                    session_id: None,
                    topics: vec![
                        FetchTopicData {
                            topic_name: "topic".to_owned(),
                            partitions: vec![
                                FetchPartitionData {
                                    partition_id: 1,
                                    error_code: 2,
                                    high_watermark: 3,
                                    last_stable_offset: Some(4),
                                    log_start_offset: None,
                                    aborted_transactions: Some(FetchAbortedTransactions {
                                        producer_id: 5,
                                        first_offset: 6,
                                    }),
                                    message_set: MessageSet {
                                        messages: vec![
                                            Message {
                                                offset: 0,
                                                compression: Compression::None,
                                                key: Some(Bytes::from(&b"key"[..])),
                                                value: Some(Bytes::from(&b"value"[..])),
                                                timestamp: Some(MessageTimestamp::LogAppendTime(456)),
                                                headers: Vec::new(),
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    ],
                }, &[
                    // ResponseHeader */
                    0, 0, 0, 123,   // correlation_id
                    // FetchResponse
                    0, 0, 0, 1,     // throttle_time
                    // topics: [TopicData]
                    0, 0, 0, 1,
                        0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                        // partitions: [PartitionData]
                        0, 0, 0, 1,
                            0, 0, 0, 1,             // partition
                            0, 2,                   // error_code
                            0, 0, 0, 0, 0, 0, 0, 3, // highwater_mark
                            0, 0, 0, 0, 0, 0, 0, 4, // last_stable_offset
                            // aborted_transactions: FetchAbortedTransactions
                                0, 0, 0, 0, 0, 0, 0, 5, // producer_id
                                0, 0, 0, 0, 0, 0, 0, 6, // first_offset
                            // MessageSet
                            0, 0, 0, 42,            // size
                                // messages: [Message]
                                0, 0, 0, 0, 0, 0, 0, 0,     // offset
                                0, 0, 0, 30,                // size
                                206, 63, 210, 11,           // crc
                                1,                          // magic
                                8,                          // attributes
                                0, 0, 0, 0, 0, 0, 1, 200,   // timestamp
                                0, 0, 0, 3, b'k', b'e', b'y',
                                0, 0, 0, 5, b'v', b'a', b'l', b'u', b'e',
                ]
            )
        ];
    }

    #[test]
    fn parse_fetch_responses() {
        use pretty_env_logger;

        let _ = pretty_env_logger::try_init();

        for &(api_version, ref response, ref data) in &*TEST_RESPONSES {
            let res = parse_fetch_response(data, api_version);

            display_parse_error::<_>(data, res.clone());

            assert_eq!(
                res,
                IResult::Done(&[][..], response.clone()),
                "fail to parse fech response, api_version {}, expected: {:#?}\n{}",
                api_version,
                response,
                hexdump!(data)
            );
        }
    }
}

use std::borrow::Cow;
use std::mem;

use bytes::{BufMut, ByteOrder, BytesMut};

use nom::{IResult, be_i16, be_i32, be_i64};

use errors::Result;
use protocol::{parse_response_header, parse_string, ApiVersion, Encodable, ErrorCode, MessageSet, MessageSetEncoder,
               Offset, ParseTag, PartitionId, Record, RecordFormat, Request, RequestHeader, RequiredAck,
               ResponseHeader, Timestamp, WriteExt, ARRAY_LEN_SIZE, BYTES_LEN_SIZE, PARTITION_ID_SIZE, STR_LEN_SIZE};

const REQUIRED_ACKS_SIZE: usize = 2;
const ACK_TIMEOUT_SIZE: usize = 4;

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceRequest<'a> {
    pub header: RequestHeader<'a>,
    /// The transactional id or null if the producer is not transactional
    pub transactional_id: Option<Cow<'a, str>>,
    /// This field indicates how many acknowledgements the servers should
    /// receive before responding to the request.
    pub required_acks: RequiredAck,
    /// This provides a maximum time in milliseconds the server can await the
    /// receipt of the number of acknowledgements in `required_acks`.
    pub ack_timeout: i32,
    /// The topic that data is being published to.
    pub topics: Vec<ProduceTopicData<'a>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceTopicData<'a> {
    /// The topic that data is being published to.
    pub topic_name: Cow<'a, str>,
    /// The partition that data is being published to.
    pub partitions: Vec<ProducePartitionData<'a>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProducePartitionData<'a> {
    /// The partition that data is being published to.
    pub partition_id: PartitionId,
    /// A set of messages in the standard format.
    pub message_set: Cow<'a, MessageSet>,
}

impl<'a> ProduceRequest<'a> {
    pub fn required_record_format(api_version: ApiVersion) -> RecordFormat {
        match api_version {
            0 | 1 => RecordFormat::V0,
            2 => RecordFormat::V1,
            3 | 4 | 5 => RecordFormat::V2,
            _ => panic!("unknown api version to use for produce request version"),
        }
    }
}

impl<'a> Request for ProduceRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + if self.header.api_version >= 3 {
            STR_LEN_SIZE + self.transactional_id.as_ref().map_or(0, |s| s.len())
        } else {
            0
        } + REQUIRED_ACKS_SIZE + ACK_TIMEOUT_SIZE + self.topics.iter().fold(ARRAY_LEN_SIZE, |size, topic| {
            size + STR_LEN_SIZE + topic.topic_name.len()
                + topic.partitions.iter().fold(ARRAY_LEN_SIZE, |size, partition| {
                    size + PARTITION_ID_SIZE + BYTES_LEN_SIZE
                        + partition.message_set.size(Self::required_record_format(api_version))
                })
        })
    }
}

impl<'a> Encodable for ProduceRequest<'a> {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        let record_format = Self::required_record_format(self.header.api_version);

        trace!(
            "encoding produce request, api version {}, record format {:?}",
            self.header.api_version,
            record_format
        );

        let encoder = MessageSetEncoder::new(record_format, None);

        self.header.encode::<T>(dst)?;

        if self.header.api_version >= 3 {
            dst.put_str::<T, _>(self.transactional_id.as_ref())?;
        }

        dst.put_i16::<T>(self.required_acks);
        dst.put_i32::<T>(self.ack_timeout);
        dst.put_array::<T, _, _>(&self.topics, |buf, topic| {
            buf.put_str::<T, _>(Some(topic.topic_name.as_ref()))?;
            buf.put_array::<T, _, _>(&topic.partitions, |buf, partition| {
                buf.put_i32::<T>(partition.partition_id);

                let size_off = buf.len();
                buf.put_i32::<T>(0);

                encoder.encode::<T>(&partition.message_set, buf)?;

                let message_set_size = buf.len() - size_off - mem::size_of::<i32>();
                T::write_i32(&mut buf[size_off..], message_set_size as i32);

                Ok(())
            })
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceResponse {
    pub header: ResponseHeader,
    /// The topic this response entry corresponds to.
    pub topics: Vec<ProduceTopicStatus>,
    /// Duration in milliseconds for which the request was throttled due to
    /// quota violation. (Zero if the request did not violate any quota).
    pub throttle_time: Option<i32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceTopicStatus {
    /// The topic this response entry corresponds to.
    pub topic_name: String,
    /// The partition this response entry corresponds to.
    pub partitions: Vec<ProducePartitionStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProducePartitionStatus {
    /// The partition this response entry corresponds to.
    pub partition_id: PartitionId,
    /// The error from this partition, if any.
    pub error_code: ErrorCode,
    /// The offset assigned to the first message in the message set appended to
    /// this partition.
    pub offset: Offset,
    /// Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970
    /// (UTC)).
    pub timestamp: Option<Timestamp>,
    /// The start offset of the log at the time this produce response was
    /// created
    pub log_start_offset: Option<Offset>,
}

impl ProduceResponse {
    pub fn parse(buf: &[u8], api_version: ApiVersion) -> IResult<&[u8], Self> {
        parse_produce_response(buf, api_version)
    }
}

named_args!(parse_produce_response(api_version: ApiVersion)<ProduceResponse>,
    parse_tag!(ParseTag::ProduceResponse,
        do_parse!(
            header: parse_response_header
         >> topics: length_count!(be_i32, apply!(parse_produce_topic_status, api_version))
         >> throttle_time: cond!(api_version > 0, be_i32)
         >> (ProduceResponse {
                header,
                topics,
                throttle_time,
            })
        )
    )
);

named_args!(parse_produce_topic_status(api_version: ApiVersion)<ProduceTopicStatus>,
    parse_tag!(ParseTag::ProduceTopicStatus,
        do_parse!(
            topic_name: parse_string
         >> partitions: length_count!(be_i32, apply!(parse_produce_partition_status, api_version))
         >> (ProduceTopicStatus {
                topic_name,
                partitions,
            })
        )
    )
);

named_args!(parse_produce_partition_status(api_version: ApiVersion)<ProducePartitionStatus>,
    parse_tag!(ParseTag::ProducePartitionStatus,
        do_parse!(
            partition_id: be_i32
         >> error_code: be_i16
         >> offset: be_i64
         >> timestamp: cond!(api_version > 1, be_i64)
         >> log_start_offset: cond!(api_version > 4, be_i64)
         >> (ProducePartitionStatus {
                partition_id,
                error_code,
                offset,
                timestamp,
                log_start_offset,
            })
        )
    )
);

#[cfg(test)]
mod tests {
    use bytes::{BigEndian, Bytes};

    use nom::IResult;

    use super::*;
    use compression::Compression;
    use protocol::*;

    lazy_static!{
        static ref TEST_REQUEST_PACKET_V0: Vec<u8> = vec![
            // ProduceRequest
                // RequestHeader
                0, 0,                               // api_key
                0, 0,                               // api_version
                0, 0, 0, 123,                       // correlation_id
                0, 6, 99, 108, 105, 101, 110, 116,  // client_id
            255, 255,                               // required_acks
            0, 0, 0, 123,                           // ack_timeout
                // topics: [ProduceTopicData]
                0, 0, 0, 1,
                    // ProduceTopicData
                    0, 5, 116, 111, 112, 105, 99,   // topic_name
                    // partitions: [ProducePartitionData]
                    0, 0, 0, 1,
                        // ProducePartitionData
                        0, 0, 0, 1,                 // partition
                        // MessageSet
                        0, 0, 0, 34,
                        // messages: [Message]
                            0, 0, 0, 0, 0, 0, 0, 0,             // offset
                            0, 0, 0, 22,                        // size
                            35, 86, 193, 55,                    // crc
                            0,                                  // magic
                            0,                                  // attributes
                            0, 0, 0, 3, 107, 101, 121,          // key
                            0, 0, 0, 5, 118, 97, 108, 117, 101  // value
        ];

        static ref TEST_REQUEST_V0: ProduceRequest<'static> = ProduceRequest {
            header: RequestHeader {
                api_key: ApiKeys::Produce as ApiVersion,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            transactional_id: None,
            required_acks: RequiredAcks::All as RequiredAck,
            ack_timeout: 123,
            topics: vec![
                ProduceTopicData {
                    topic_name: "topic".into(),
                    partitions: vec![
                        ProducePartitionData {
                            partition_id: 1,
                            message_set: Cow::Owned(MessageSet {
                                messages: vec![
                                    Message {
                                        offset: 0,
                                        compression: Compression::None,
                                        key: Some(Bytes::from(&b"key"[..])),
                                        value: Some(Bytes::from(&b"value"[..])),
                                        timestamp: Some(MessageTimestamp::CreateTime(456)),
                                        headers: Vec::new(),
                                    },
                                ],
                            }),
                        },
                    ],
                },
            ],
        };

        static ref TEST_REQUEST_PACKET_V2: Vec<u8> = vec![
            // ProduceRequest
                // RequestHeader
                0, 0,                               // api_key
                0, 2,                               // api_version
                0, 0, 0, 123,                       // correlation_id
                0, 6, 99, 108, 105, 101, 110, 116,  // client_id
            255, 255,                               // required_acks
            0, 0, 0, 123,                           // ack_timeout
                // topics: [ProduceTopicData]
                0, 0, 0, 1,
                    // ProduceTopicData
                    0, 5, 116, 111, 112, 105, 99,   // topic_name
                    // partitions: [ProducePartitionData]
                    0, 0, 0, 1,
                        // ProducePartitionData
                        0, 0, 0, 1,                 // partition
                        // MessageSet
                        0, 0, 0, 42,
                        // messages: [Message]
                            0, 0, 0, 0, 0, 0, 0, 0,             // offset
                            0, 0, 0, 30,                        // size
                            226, 52, 65, 188,                   // crc
                            1,                                  // magic
                            0,                                  // attributes
                            0, 0, 0, 0, 0, 0, 1, 200,           // timestamp
                            0, 0, 0, 3, 107, 101, 121,          // key
                            0, 0, 0, 5, 118, 97, 108, 117, 101  // value
        ];

        static ref TEST_REQUEST_V2: ProduceRequest<'static> = ProduceRequest {
            header: RequestHeader {
                api_key: ApiKeys::Produce as ApiVersion,
                api_version: 2,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            transactional_id: None,
            required_acks: RequiredAcks::All as RequiredAck,
            ack_timeout: 123,
            topics: vec![
                ProduceTopicData {
                    topic_name: "topic".into(),
                    partitions: vec![
                        ProducePartitionData {
                            partition_id: 1,
                            message_set: Cow::Owned(MessageSet {
                                messages: vec![
                                    Message {
                                        offset: 0,
                                        compression: Compression::None,
                                        key: Some(Bytes::from(&b"key"[..])),
                                        value: Some(Bytes::from(&b"value"[..])),
                                        timestamp: Some(MessageTimestamp::CreateTime(456)),
                                        headers: Vec::new(),
                                    },
                                ],
                            }),
                        },
                    ],
                },
            ],
        };

        #[cfg_attr(rustfmt, rustfmt_skip)]
        static ref TEST_REQUEST_PACKET_V3: Vec<u8> = vec![
            // ProduceRequest
                // RequestHeader
                0, 0,                                       // api_key
                0, 3,                                       // api_version
                0, 0, 0, 123,                               // correlation_id
                0, 6, b'c', b'l', b'i', b'e', b'n', b't',   // client_id
            0, 2, b'i', b'd',                               // transactional_id
            255, 255,                                       // required_acks
            0, 0, 0, 123,                                   // ack_timeout
                // topics: [ProduceTopicData]
                0, 0, 0, 1,
                    // ProduceTopicData
                    0, 5, b't', b'o', b'p', b'i', b'c',     // topic_name
                    // partitions: [ProducePartitionData]
                    0, 0, 0, 1,
                        // ProducePartitionData
                        0, 0, 0, 1,                 // partition
                        // Record Batch
                        0, 0, 0, 86,
                        // records: [RecordBody]
                        //      first offset
                        0, 0, 0, 0, 0, 0, 0, 0,
                        //      length
                        0, 0, 0, 49,
                        //      partition_leader_epoch
                        0, 0, 0, 0,
                        //      magic
                        2,
                        //      crc
                        218, 150, 207, 117,
                        //      attributes
                        0, 0,
                        //      last_offset_delta
                        0, 0, 0, 0,
                        //      first_timestamp
                        0, 0, 0, 0, 0, 0, 1, 200,
                        //      max_timestamp
                        0, 0, 0, 0, 0, 0, 1, 200,
                        //      producer_id
                        0, 0, 0, 0, 0, 0, 0, 0,
                        //      producer_epoch
                        0, 0,
                        //      first_sequence
                        0, 0, 0, 0,
                        //      records count
                        0, 0, 0, 1,
                        //      record
                        //          length
                        50,
                        //          attributes
                        0,
                        //          timestamp_delta
                        0,
                        //          offset_delta
                        0,
                        //          key
                        6, b'k', b'e', b'y',
                        //          value
                        10, b'v', b'a', b'l', b'u', b'e',
                        //          headers count
                        2,
                        //          header
                        //              key
                        6, b'k', b'e', b'y',
                        //              value
                        10, b'v', b'a', b'l', b'u', b'e',

        ];

        static ref TEST_REQUEST_V3: ProduceRequest<'static> = ProduceRequest {
            header: RequestHeader {
                api_key: ApiKeys::Produce as ApiVersion,
                api_version: 3,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            transactional_id: Some("id".into()),
            required_acks: RequiredAcks::All as RequiredAck,
            ack_timeout: 123,
            topics: vec![
                ProduceTopicData {
                    topic_name: "topic".into(),
                    partitions: vec![
                        ProducePartitionData {
                            partition_id: 1,
                            message_set: Cow::Owned(MessageSet {
                                messages: vec![
                                    Message {
                                        offset: 0,
                                        compression: Compression::None,
                                        key: Some(Bytes::from(&b"key"[..])),
                                        value: Some(Bytes::from(&b"value"[..])),
                                        timestamp: Some(MessageTimestamp::CreateTime(456)),
                                        headers: vec![
                                            RecordHeader{
                                                key: "key".to_owned(),
                                                value: Some(Bytes::from(&b"value"[..]))
                                            }
                                        ],
                                    },
                                ],
                            }),
                        },
                    ],
                },
            ],
        };

        static ref TEST_RESPONSE_PACKET_V0: Vec<u8> = vec![
            // ResponseHeader
            0, 0, 0, 123, // correlation_id
            // topics: [ProduceTopicStatus]
            0, 0, 0, 1,
                0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                // partitions: [ProducePartitionStatus]
                0, 0, 0, 1,
                    0, 0, 0, 1,             // partition
                    0, 2,                   // error_code
                    0, 0, 0, 0, 0, 0, 0, 3, // offset
                    0, 0, 0, 0, 0, 0, 0, 4, // timestamp
            0, 0, 0, 5 // throttle_time
        ];

        static ref TEST_RESPONSE: ProduceResponse = ProduceResponse {
            header: ResponseHeader { correlation_id: 123 },
            topics: vec![ProduceTopicStatus {
                             topic_name: "topic".to_owned(),
                             partitions: vec![ProducePartitionStatus {
                                                  partition_id: 1,
                                                  error_code: 2,
                                                  offset: 3,
                                                  timestamp: Some(4),
                                                  log_start_offset: None,
                                              }],
                         }],
            throttle_time: Some(5),
        };
    }

    #[test]
    fn test_encode_produce_request_v0() {
        let req = &*TEST_REQUEST_V0;
        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();
        let req_size = req.size(req.header.api_version);

        assert_eq!(
            req_size,
            buf.len(),
            "encoded request to {} bytes, expected {} bytes, request: {:#?}\nencoded buffer:\n{}\n",
            buf.len(),
            req_size,
            req,
            hexdump!(&buf)
        );

        assert_eq!(&buf[..], &TEST_REQUEST_PACKET_V0[..]);
    }

    #[test]
    fn test_encode_produce_request_v2() {
        let req = &*TEST_REQUEST_V2;
        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();
        let req_size = req.size(req.header.api_version);

        assert_eq!(
            req_size,
            buf.len(),
            "encoded request to {} bytes, expected {} bytes, request: {:#?}\nencoded buffer:\n{}\n",
            buf.len(),
            req_size,
            req,
            hexdump!(&buf)
        );

        assert_eq!(&buf[..], &TEST_REQUEST_PACKET_V2[..]);
    }

    #[test]
    fn test_encode_produce_request_v3() {
        let req = &*TEST_REQUEST_V3;
        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();
        let req_size = req.size(req.header.api_version);

        assert_eq!(
            req_size,
            buf.len(),
            "encoded request to {} bytes, expected {} bytes, request: {:#?}\nencoded buffer:\n{}\n",
            buf.len(),
            req_size,
            req,
            hexdump!(&buf)
        );

        assert_eq!(&buf[..], &TEST_REQUEST_PACKET_V3[..]);
    }

    #[test]
    fn test_parse_produce_response() {
        assert_eq!(
            parse_produce_response(TEST_RESPONSE_PACKET_V0.as_slice(), 2),
            IResult::Done(&[][..], TEST_RESPONSE.clone())
        );
    }
}

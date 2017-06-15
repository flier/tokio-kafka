use std::borrow::Cow;
use std::mem;

use bytes::{BufMut, ByteOrder, BytesMut};

use nom::{IResult, be_i16, be_i32, be_i64};

use errors::Result;
use protocol::{ARRAY_LEN_SIZE, ApiVersion, BYTES_LEN_SIZE, Encodable, ErrorCode, MessageSet,
               MessageSetEncoder, Offset, PARTITION_ID_SIZE, ParseTag, PartitionId, Record,
               RequestHeader, RequiredAck, ResponseHeader, STR_LEN_SIZE, Timestamp, WriteExt,
               parse_response_header, parse_string};

const REQUIRED_ACKS_SIZE: usize = 2;
const ACK_TIMEOUT_SIZE: usize = 4;

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceRequest<'a> {
    pub header: RequestHeader<'a>,
    pub required_acks: RequiredAck,
    pub ack_timeout: i32,
    pub topics: Vec<ProduceTopicData<'a>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceTopicData<'a> {
    pub topic_name: Cow<'a, str>,
    pub partitions: Vec<ProducePartitionData<'a>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProducePartitionData<'a> {
    pub partition_id: PartitionId,
    pub message_set: Cow<'a, MessageSet>,
}

impl<'a> Record for ProduceRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + REQUIRED_ACKS_SIZE + ACK_TIMEOUT_SIZE +
            self.topics.iter().fold(ARRAY_LEN_SIZE, |size, topic| {
                size + STR_LEN_SIZE + topic.topic_name.len() +
                    topic.partitions.iter().fold(
                        ARRAY_LEN_SIZE,
                        |size, partition| {
                            size + PARTITION_ID_SIZE + BYTES_LEN_SIZE +
                                partition.message_set.size(api_version)
                        },
                    )
            })
    }
}

impl<'a> Encodable for ProduceRequest<'a> {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        let encoder = MessageSetEncoder::new(self.header.api_version, None);

        self.header.encode::<T>(dst)?;

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
    pub topics: Vec<ProduceTopicStatus>,
    pub throttle_time: Option<i32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceTopicStatus {
    pub topic_name: String,
    pub partitions: Vec<ProducePartitionStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProducePartitionStatus {
    pub partition_id: PartitionId,
    pub error_code: ErrorCode,
    pub offset: Offset,
    pub timestamp: Option<Timestamp>,
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
                header: header,
                topics: topics,
                throttle_time: throttle_time,
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
                topic_name: topic_name,
                partitions: partitions,
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
         >> (ProducePartitionStatus {
                partition_id: partition_id,
                error_code: error_code,
                offset: offset,
                timestamp: timestamp,
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
        static ref TEST_REQUEST_DATA: Vec<u8> = vec![
            // ProduceRequest
                // RequestHeader
                0, 0,                               // api_key
                0, 1,                               // api_version
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

        static ref TEST_RESPONSE_DATA: Vec<u8> = vec![
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
                                              }],
                         }],
            throttle_time: Some(5),
        };
    }

    #[test]
    fn test_encode_produce_request() {
        let req = ProduceRequest {
            header: RequestHeader {
                api_key: ApiKeys::Produce as ApiVersion,
                api_version: 1,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            required_acks: RequiredAcks::All as RequiredAck,
            ack_timeout: 123,
            topics: vec![ProduceTopicData {
                topic_name: "topic".into(),
                partitions: vec![ProducePartitionData {
                    partition_id: 1,
                    message_set: Cow::Owned(MessageSet {
                        messages: vec![Message {
                            offset: 0,
                            compression: Compression::None,
                            key: Some(Bytes::from(&b"key"[..])),
                            value: Some(Bytes::from(&b"value"[..])),
                            timestamp: Some(MessageTimestamp::CreateTime(456)),
                        }],
                    }),
                }],
            }],
        };

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &TEST_REQUEST_DATA[..]);
    }

    #[test]
    fn test_parse_produce_response() {
        assert_eq!(parse_produce_response(TEST_RESPONSE_DATA.as_slice(), 2),
                   IResult::Done(&[][..], TEST_RESPONSE.clone()));
    }
}

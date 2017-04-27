use bytes::{BytesMut, BufMut, ByteOrder};

use nom::{be_i16, be_i32, be_i64};

use errors::Result;
use protocol::{Encodable, RequestHeader, ResponseHeader, MessageSet, MessageSetEncoder, ParseTag,
               parse_string, parse_response_header, WriteExt};

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceRequest {
    pub header: RequestHeader,
    pub required_acks: i16,
    pub timeout: i32,
    pub topics: Vec<ProduceTopicData>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceTopicData {
    pub topic_name: String,
    pub partitions: Vec<ProducePartitionData>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProducePartitionData {
    pub partition: i32,
    pub message_set: MessageSet,
}

impl Encodable for ProduceRequest {
    fn encode<T: ByteOrder>(self, dst: &mut BytesMut) -> Result<()> {
        let encoder = MessageSetEncoder::new(self.header.api_version);

        self.header.encode::<T>(dst)?;

        dst.put_i16::<T>(self.required_acks);
        dst.put_i32::<T>(self.timeout);
        dst.put_array::<T, _, _>(self.topics, |buf, topic| {
            buf.put_str::<T, _>(Some(topic.topic_name))?;
            buf.put_array::<T, _, _>(topic.partitions, |buf, partition| {
                buf.put_i32::<T>(partition.partition);
                encoder.encode::<T>(partition.message_set, buf)
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
    pub partition: i32,
    pub error_code: i16,
    pub offset: i64,
    pub timestamp: Option<i64>,
}

named_args!(pub parse_produce_response(api_version: i16)<ProduceResponse>,
    do_parse!(
        header: parse_response_header
     >> topics: parse_tag!(ParseTag::ProduceTopics,
            length_count!(be_i32, apply!(parse_produce_topic_status, api_version)))
     >> throttle_time: cond!(api_version > 0, be_i32)
     >> (ProduceResponse {
            header: header,
            topics: topics,
            throttle_time: throttle_time,
        })
    )
);

named_args!(parse_produce_topic_status(api_version: i16)<ProduceTopicStatus>,
    do_parse!(
        topic_name: parse_string
     >> partitions: parse_tag!(ParseTag::ProducePartitions,
            length_count!(be_i32, apply!(parse_produce_partition_status, api_version)))
     >> (ProduceTopicStatus {
            topic_name: topic_name,
            partitions: partitions,
        })
    )
);

named_args!(parse_produce_partition_status(api_version: i16)<ProducePartitionStatus>,
    do_parse!(
        partition: be_i32
     >> error_code: be_i16
     >> offset: be_i64
     >> timestamp: cond!(api_version > 1, be_i64)
     >> (ProducePartitionStatus {
            partition: partition,
            error_code: error_code,
            offset: offset,
            timestamp: timestamp,
        })
    )
);

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BigEndian};

    use nom::IResult;

    use super::*;
    use protocol::*;
    use compression::Compression;

    lazy_static!{
        static ref TEST_REQUEST_DATA: Vec<u8> = vec![
            // ProduceRequest
                // RequestHeader
                0, 0,                               // api_key
                0, 2,                               // api_version
                0, 0, 0, 123,                       // correlation_id
                0, 6, 99, 108, 105, 101, 110, 116,  // client_id
            255, 255,                               // required_acks
            0, 0, 0, 123,                           // timeout
                // topics: [ProduceTopicData]
                0, 0, 0, 1,
                    // ProduceTopicData
                    0, 5, 116, 111, 112, 105, 99,   // topic_name
                    // partitions: [ProducePartitionData]
                    0, 0, 0, 1,
                        // ProducePartitionData
                        0, 0, 0, 1,                 // partition
                        // MessageSet
                        0, 0, 0, 1,
                        // messages: [Message]
                        0, 0, 0, 0, 0, 0, 0, 0,             // offset
                        0, 0, 0, 30,                        // size
                        226, 52, 65, 188,                   // crc
                        MAGIC_BYTE as u8,                   // magic
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
                                                  partition: 1,
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
                api_key: ApiKeys::Produce as i16,
                api_version: 2,
                correlation_id: 123,
                client_id: Some("client".to_owned()),
            },
            required_acks: RequiredAcks::All as i16,
            timeout: 123,
            topics: vec![ProduceTopicData {
                topic_name: "topic".to_owned(),
                partitions: vec![ProducePartitionData {
                    partition: 1,
                    message_set: MessageSet {
                        messages: vec![Message {
                            offset: 0,
                            compression: Compression::None,
                            key: Some(Bytes::from(&b"key"[..])),
                            value: Some(Bytes::from(&b"value"[..])),
                            timestamp: Some(456),
                        }],
                    },
                }],
            }],
        };

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(&buf[..], &TEST_REQUEST_DATA[..]);
    }

    #[test]
    fn test_parse_produce_response() {
        assert_eq!(parse_produce_response(TEST_RESPONSE_DATA.as_slice(), 2),
                   IResult::Done(&[][..], TEST_RESPONSE.clone()));
    }
}
 #![allow(dead_code)]

use std::marker::PhantomData;

use bytes::{BytesMut, BufMut, ByteOrder};

use nom::{be_i16, be_i32, be_i64};

use tokio_io::codec::Encoder;

use errors::{Error, Result};
use codec::WriteExt;
use compression::Compression;

use protocol::{RequestHeader, ResponseHeader, MessageSet, parse_str, parse_response_header};

#[derive(Debug, PartialEq)]
pub struct ProduceRequest<'a> {
    pub header: RequestHeader<'a>,
    pub required_acks: i16,
    pub timeout: i32,
    pub topics: Vec<ProduceTopicData<'a>>,
}

#[derive(Debug, PartialEq)]
pub struct ProduceTopicData<'a> {
    pub topic_name: &'a str,
    pub partitions: Vec<ProducePartitionData<'a>>,
}

#[derive(Debug, PartialEq)]
pub struct ProducePartitionData<'a> {
    pub partition: i32,
    pub message_set: MessageSet<'a>,
}

#[derive(Debug, PartialEq)]
pub struct ProduceRequestEncoder<'a, T: 'a> {
    pub offset: i64,
    pub api_version: i8,
    pub compression: Compression,
    pub phantom: PhantomData<&'a T>,
}

impl<'a, T> ProduceRequestEncoder<'a, T> {
    fn next_offset(&mut self) -> i64 {
        match self.compression {
            Compression::None => 0,
            _ => {
                let offset = self.offset;
                self.offset.wrapping_add(1);
                offset
            }
        }
    }
}

impl<'a, T: 'a + ByteOrder> Encoder for ProduceRequestEncoder<'a, T> {
    type Item = ProduceRequest<'a>;
    type Error = Error;

    fn encode(&mut self, req: Self::Item, dst: &mut BytesMut) -> Result<()> {
        dst.put_item::<T, _>(&req.header)?;
        dst.put_i16::<T>(req.required_acks);
        dst.put_i32::<T>(req.timeout);
        dst.put_array::<T, _, _>(&req.topics[..], |buf, topic| {
            buf.put_str::<T, _>(topic.topic_name)?;
            buf.put_array::<T, _, _>(&topic.partitions, |buf, partition| {
                buf.put_i32::<T>(partition.partition);
                buf.put_array::<T, _, _>(&partition.message_set.messages, |buf, message| {
                    message.encode::<T>(buf, self.next_offset(), self.api_version, self.compression)
                })
            })
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct ProduceResponse<'a> {
    pub header: ResponseHeader,
    pub topics: Vec<ProduceTopicStatus<'a>>,
    pub throttle_time: Option<i32>,
}

#[derive(Debug, PartialEq)]
pub struct ProduceTopicStatus<'a> {
    pub topic_name: Option<&'a str>,
    pub partitions: Vec<ProducePartitionStatus>,
}

#[derive(Debug, PartialEq)]
pub struct ProducePartitionStatus {
    pub partition: i32,
    pub error_code: i16,
    pub offset: i64,
    pub timestamp: Option<i64>,
}

named_args!(pub parse_produce_response(version: u8)<ProduceResponse>,
    do_parse!(
        header: parse_response_header
     >> n: be_i32
     >> topics: many_m_n!(n as usize, n as usize, apply!(parse_produce_topic_status, version))
     >> throttle_time: cond!(version > 0, be_i32)
     >> (ProduceResponse {
            header: header,
            topics: topics,
            throttle_time: throttle_time,
        })
    )
);

named_args!(pub parse_produce_topic_status(version: u8)<ProduceTopicStatus>,
    do_parse!(
        name: parse_str
     >> n: be_i32
     >> partitions: many_m_n!(n as usize, n as usize, apply!(parse_produce_partition_status, version))
     >> (ProduceTopicStatus {
            topic_name: name,
            partitions: partitions,
        })
    )
);

named_args!(pub parse_produce_partition_status(version: u8)<ProducePartitionStatus>,
    do_parse!(
        partition: be_i32
     >> error_code: be_i16
     >> offset: be_i64
     >> timestamp: cond!(version > 1, be_i64)
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
    use bytes::BigEndian;

    use nom::IResult;

    use super::*;
    use protocol::*;

    #[test]
    fn test_produce_request() {
        let req = ProduceRequest {
            header: RequestHeader {
                api_key: ApiKeys::Fetch as i16,
                api_version: 2,
                correlation_id: 123,
                client_id: Some("client"),
            },
            required_acks: RequiredAcks::All as i16,
            timeout: 123,
            topics: vec![ProduceTopicData {
                             topic_name: "topic",
                             partitions: vec![ProducePartitionData {
                                                  partition: 1,
                                                  message_set: MessageSet {
                                                      messages: vec![Message {
                                                                         key: Some(b"key"),
                                                                         value: Some(b"value"),
                                                                         timestamp: Some(456),
                                                                     }],
                                                  },
                                              }],
                         }],
        };

        let mut encoder = ProduceRequestEncoder::<BigEndian> {
            offset: 0,
            api_version: 1,
            compression: Compression::None,
            phantom: PhantomData,
        };

        let mut buf = BytesMut::with_capacity(128);

        encoder.encode(req, &mut buf).unwrap();

        assert_eq!(&buf[..],
                   &[
            // ProduceRequest
                // RequestHeader
                0, 1,                               // api_key
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
                        1,                                  // magic
                        0,                                  // attributes
                        0, 0, 0, 0, 0, 0, 1, 200,           // timestamp
                        0, 0, 0, 3, 107, 101, 121,          // key
                        0, 0, 0, 5, 118, 97, 108, 117, 101  // value
        ]
                        [..]);
    }

    #[test]
    fn test_produce_response() {
        let res = ProduceResponse {
            header: ResponseHeader { correlation_id: 123 },
            topics: vec![ProduceTopicStatus {
                             topic_name: Some("topic"),
                             partitions: vec![ProducePartitionStatus {
                                                  partition: 1,
                                                  error_code: 2,
                                                  offset: 3,
                                                  timestamp: Some(4),
                                              }],
                         }],
            throttle_time: Some(5),
        };

        assert_eq!(parse_produce_response(&[
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
        ],
                                          2),
                   IResult::Done(&b""[..], res));
    }
}
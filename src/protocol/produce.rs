 #![allow(dead_code)]

use std::marker::PhantomData;

use bytes::{BytesMut, BufMut, ByteOrder};

use tokio_io::codec::Encoder;

use errors::{Error, Result};
use codec::WriteExt;
use compression::Compression;

use protocol::header::{RequestHeader, ResponseHeader};
use protocol::message::MessageSet;

#[derive(Debug)]
pub struct ProduceRequest<'a> {
    pub header: RequestHeader<'a>,
    pub required_acks: i16,
    pub timeout: i32,
    pub topics: Vec<ProduceTopicData<'a>>,
}

#[derive(Debug)]
pub struct ProduceTopicData<'a> {
    pub topic_name: &'a str,
    pub partitions: Vec<ProducePartitionData<'a>>,
}

#[derive(Debug)]
pub struct ProducePartitionData<'a> {
    pub partition: i32,
    pub message_set: MessageSet<'a>,
}

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

#[derive(Debug)]
pub struct ProduceResponse<'a> {
    pub header: ResponseHeader,
    pub topics: Vec<ProduceTopicStatus<'a>>,
    pub throttle_time: Option<i32>,
}

#[derive(Debug)]
pub struct ProduceTopicStatus<'a> {
    pub topic_name: &'a str,
    pub partitions: Vec<ProducePartitionStatus>,
}

#[derive(Debug)]
pub struct ProducePartitionStatus {
    pub partition: i32,
    pub error_code: i16,
    pub offset: i64,
    pub timestamp: Option<i64>,
}

#[cfg(test)]
mod tests {
    use bytes::BigEndian;

    use super::*;
    use protocol::*;

    #[test]
    fn request_header() {
        let hdr = RequestHeader {
            api_key: ApiKeys::Fetch as i16,
            api_version: 2,
            correlation_id: 123,
            client_id: Some("test"),
        };

        let mut buf = vec![];

        buf.put_item::<BigEndian, _>(&hdr).unwrap();

        assert_eq!(&buf[..],
                   &[0, 1,            // api_key
                               0, 2,            // api_version
                               0, 0, 0, 123,    // correlation_id
                               0, 4, 116, 101, 115, 116]);

        let bytes = &[0, 0, 0, 123];
        let res = parse_response_header(bytes);

        assert!(res.is_done());

        let (remaning, hdr) = res.unwrap();

        assert_eq!(remaning, b"");
        assert_eq!(hdr.correlation_id, 123);
    }

    #[test]
    fn produce_request() {
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
}
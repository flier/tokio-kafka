 #![allow(dead_code)]

use std::marker::PhantomData;

use bytes::{BytesMut, BufMut, ByteOrder};

use nom::{IResult, be_i32};

use time;

use crc::crc32;

use tokio_io::codec::{Encoder, Decoder};

use errors::{Error, Result};
use codec::{Encodable, WriteExt};
use compression::Compression;

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


#[derive(Debug)]
pub struct RequestHeader<'a> {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<&'a str>,
}

impl<'a> Encodable for RequestHeader<'a> {
    fn encode<T: ByteOrder, B: BufMut>(&self, mut buf: B) -> Result<()> {
        buf.put_i16::<T>(self.api_key);
        buf.put_i16::<T>(self.api_version);
        buf.put_i32::<T>(self.correlation_id);
        buf.put_str::<T, _>(self.client_id)
    }
}

#[derive(Debug)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

named!(parse_response_header<ResponseHeader>,
    do_parse!(
        correlation_id: be_i32
     >> (
            ResponseHeader {
                correlation_id: correlation_id,
            }
        )
    )
);

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

/// Message sets
///
/// One structure common to both the produce and fetch requests is the message set format.
/// A message in kafka is a key-value pair with a small amount of associated metadata.
/// A message set is just a sequence of messages with offset and size information.
///  This format happens to be used both for the on-disk storage on the broker and the on-the-wire format.
///
/// MessageSet => [Offset MessageSize Message]
///   Offset => int64
///   MessageSize => int32
#[derive(Debug)]
pub struct MessageSet<'a> {
    pub messages: Vec<Message<'a>>,
}

/// Message format
///
/// v0
/// Message => Crc MagicByte Attributes Key Value
///   Crc => int32
///   MagicByte => int8
///   Attributes => int8
///   Key => bytes
///   Value => bytes
///
/// v1 (supported since 0.10.0)
/// Message => Crc MagicByte Attributes Key Value
///   Crc => int32
///   MagicByte => int8
///   Attributes => int8
///   Timestamp => int64
///   Key => bytes
///   Value => bytes
#[derive(Debug)]
pub struct Message<'a> {
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub timestamp: Option<i64>,
}

impl<'a> Message<'a> {
    fn encode<T: ByteOrder>(&self,
                            buf: &mut BytesMut,
                            offset: i64,
                            version: i8,
                            compression: Compression)
                            -> Result<()> {
        buf.put_i64::<T>(offset);
        let size_off = buf.len();
        buf.put_i32::<T>(0);
        let crc_off = buf.len();
        buf.put_i32::<T>(0);
        let data_off = buf.len();
        buf.put_i8(version);
        buf.put_i8(compression as i8);

        if version > 0 {
            buf.put_i64::<T>(self.timestamp
                                 .unwrap_or_else(|| {
                                                     let ts = time::now_utc().to_timespec();
                                                     ts.sec * 1000_000 + ts.nsec as i64 / 1000
                                                 }));
        }

        buf.put_bytes::<T, _>(self.key)?;
        buf.put_bytes::<T, _>(self.value)?;

        let size = buf.len() - crc_off;
        let crc = crc32::checksum_ieee(&buf[data_off..]);

        T::write_i32(&mut buf[size_off..], size as i32);
        T::write_i32(&mut buf[crc_off..], crc as i32);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::BigEndian;

    use super::*;

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

        assert_eq!(&buf[..], &[0, 1,            // api_key
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

        assert_eq!(&buf[..], &[
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
        ][..]);
    }
}
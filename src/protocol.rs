use bytes::{BytesMut, BufMut, ByteOrder};

use nom::{IResult, be_i32};

use time;

use tokio_io::codec::{Encoder, Decoder};

use errors::{Error, Result};
use codec::{Encodable, WriteExt};
use compression::Compression;

/// The following are the numeric codes that the ApiKey in the request can take for each of the below request types.
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

impl Decoder for ResponseHeader {
    type Item = ResponseHeader;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        match parse_response_header(src) {
            IResult::Done(_, header) => Ok(Some(header)),
            IResult::Incomplete(_) => Ok(None),
            IResult::Error(err) => bail!(err),
        }
    }
}

#[derive(Debug)]
pub struct ProduceRequest<'a> {
    pub header: RequestHeader<'a>,
    pub acks: i16,
    pub timeout: i32,
    pub topics: Vec<ProduceTopicData<'a>>,
}

impl<'a> Encodable for ProduceRequest<'a> {
    fn encode<T: ByteOrder, B: BufMut>(&self, mut buf: B) -> Result<()> {
        buf.put_item::<T, _>(&self.header)?;
        buf.put_i16::<T>(self.acks);
        buf.put_i32::<T>(self.timeout);
        buf.put_array::<T, _>(&self.topics)
    }
}

#[derive(Debug)]
pub struct ProduceTopicData<'a> {
    pub topic_name: &'a str,
    pub partitions: Vec<ProducePartitionData<'a>>,
}

impl<'a> Encodable for ProduceTopicData<'a> {
    fn encode<T: ByteOrder, B: BufMut>(&self, mut buf: B) -> Result<()> {
        buf.put_str::<T, _>(self.topic_name)?;
        buf.put_array::<T, _>(&self.partitions)
    }
}

#[derive(Debug)]
pub struct ProducePartitionData<'a> {
    pub partition: i32,
    pub message_set: MessageSet<'a>,
}

impl<'a> Encodable for ProducePartitionData<'a> {
    fn encode<T: ByteOrder, B: BufMut>(&self, mut buf: B) -> Result<()> {
        buf.put_i32::<T>(self.partition);
        buf.put_item::<T, _>(&self.message_set)
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

impl<'a> Encodable for MessageSet<'a> {
    fn encode<T: ByteOrder, B: BufMut>(&self, mut buf: B) -> Result<()> {
        buf.put_array::<T, _>(&self.messages[..])
    }
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
    fn internal_encode<T: ByteOrder, B: BufMut>(&self,
                                                mut buf: B,
                                                version: i8,
                                                compression: Compression)
                                                -> Result<()> {
        let offset = 0;
        let size = 0;
        let crc = 0;

        buf.put_i64::<T>(offset);
        buf.put_i32::<T>(size);
        buf.put_i32::<T>(crc);
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

        Ok(())
    }
}

impl<'a> Encodable for Message<'a> {
    fn encode<T: ByteOrder, B: BufMut>(&self, buf: B) -> Result<()> {
        self.internal_encode::<T, B>(buf, 0, Compression::None)
    }
}

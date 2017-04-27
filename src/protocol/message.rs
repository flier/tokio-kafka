use bytes::{Bytes, BytesMut, BufMut, ByteOrder};

use nom::{be_i8, be_i32, be_i64};

use time;

use crc::crc32;

use errors::Result;
use compression::Compression;
use protocol::{WriteExt, parse_bytes};

pub const MAGIC_BYTE: i8 = 1;

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
#[derive(Clone, Debug, PartialEq)]
pub struct MessageSet {
    pub messages: Vec<Message>,
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
#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub compression: Compression,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

pub struct MessageSetEncoder {
    api_version: i16,
}

impl MessageSetEncoder {
    pub fn new(api_version: i16) -> Self {
        MessageSetEncoder { api_version: api_version }
    }

    pub fn encode<T: ByteOrder>(&self, message_set: MessageSet, buf: &mut BytesMut) -> Result<()> {
        let mut offset: i64 = 0;

        buf.put_array::<T, _, _>(message_set.messages, move |buf, message| {
            let offset = if message.compression == Compression::None {
                message.offset
            } else {
                offset = offset.wrapping_add(1);
                offset - 1
            };

            self.encode_message::<T>(message, offset, buf)
        })
    }

    fn encode_message<T: ByteOrder>(&self,
                                    message: Message,
                                    offset: i64,
                                    buf: &mut BytesMut)
                                    -> Result<()> {
        buf.put_i64::<T>(offset);
        let size_off = buf.len();
        buf.put_i32::<T>(0);
        let crc_off = buf.len();
        buf.put_i32::<T>(0);
        let data_off = buf.len();
        buf.put_i8(MAGIC_BYTE);
        buf.put_i8(message.compression as i8);

        if self.api_version > 0 {
            buf.put_i64::<T>(message
                                 .timestamp
                                 .unwrap_or_else(|| {
                                                     let ts = time::now_utc().to_timespec();
                                                     ts.sec * 1000_000 + ts.nsec as i64 / 1000
                                                 }));
        }

        buf.put_bytes::<T, _>(message.key)?;
        buf.put_bytes::<T, _>(message.value)?;

        let size = buf.len() - crc_off;
        let crc = crc32::checksum_ieee(&buf[data_off..]);

        T::write_i32(&mut buf[size_off..], size as i32);
        T::write_i32(&mut buf[crc_off..], crc as i32);

        Ok(())
    }
}

named_args!(pub parse_message_set(api_version: i16)<MessageSet>,
    do_parse!(
        messages: length_count!(be_i32, apply!(parse_message, api_version))
     >> (MessageSet {
            messages: messages,
        })
    )
);

named_args!(parse_message(api_version: i16)<Message>,
    do_parse!(
        offset: be_i64
     >> size: be_i32
     >> crc: be_i32
     >> magic: be_i8
     >> attrs: be_i8
     >> timestamp: cond!(api_version > 0, be_i64)
     >> key: parse_bytes
     >> value: parse_bytes
     >> (Message {
            offset: offset,
            timestamp: timestamp,
            compression: Compression::from(attrs & 0x07),
            key: key,
            value: value,
        })
    )
);
use std::mem;
use std::ops::Deref;

use bytes::{BufMut, ByteOrder, Bytes, BytesMut};

use nom::{be_i32, be_i64, be_i8};

use time;

use crc::crc32;

use errors::{ErrorKind, Result};
use compression::Compression;
use protocol::{ApiVersion, Offset, ParseTag, Timestamp, WriteExt, parse_opt_bytes};

pub const TIMESTAMP_TYPE_MASK: i8 = 0x08;
pub const COMPRESSION_CODEC_MASK: i8 = 0x07;

const CRC_LENGTH: usize = 4;
const MAGIC_LENGTH: usize = 1;
const ATTRIBUTE_LENGTH: usize = 1;
const TIMESTAMP_LENGTH: usize = 8;
const KEY_SIZE_LENGTH: usize = 4;
const VALUE_SIZE_LENGTH: usize = 4;
const RECORD_HEADER_SIZE: usize = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH;
const RECORD_OVERHEAD_V0: usize = RECORD_HEADER_SIZE + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;
const RECORD_OVERHEAD_V1: usize = RECORD_HEADER_SIZE + TIMESTAMP_LENGTH + KEY_SIZE_LENGTH +
                                  VALUE_SIZE_LENGTH;

const COMPRESSION_RATE_ESTIMATION_FACTOR: f32 = 1.05;

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

impl Deref for MessageSet {
    type Target = [Message];

    fn deref(&self) -> &Self::Target {
        self.messages.as_slice()
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
#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    pub offset: Offset,
    pub timestamp: Option<MessageTimestamp>,
    pub compression: Compression,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MessageTimestamp {
    CreateTime(Timestamp),
    LogAppendTime(Timestamp),
}

impl MessageTimestamp {
    pub fn value(&self) -> Timestamp {
        match self {
            &MessageTimestamp::CreateTime(v) |
            &MessageTimestamp::LogAppendTime(v) => v,
        }
    }
}

impl Default for MessageTimestamp {
    fn default() -> Self {
        let ts = time::now_utc().to_timespec();

        MessageTimestamp::CreateTime(ts.sec * 1000_000 + ts.nsec as Timestamp / 1000)
    }
}

pub struct MessageSetEncoder {
    api_version: ApiVersion,
}

impl MessageSetEncoder {
    pub fn new(api_version: ApiVersion) -> Self {
        MessageSetEncoder { api_version: api_version }
    }

    pub fn encode<T: ByteOrder>(&self, message_set: MessageSet, buf: &mut BytesMut) -> Result<()> {
        let mut offset: Offset = 0;

        let size_off = buf.len();
        buf.put_i32::<T>(0);

        for message in message_set.messages {
            let offset = if message.compression == Compression::None {
                message.offset
            } else {
                offset = offset.wrapping_add(1);
                offset - 1
            };

            self.encode_message::<T>(message, offset, buf)?;
        }

        let message_set_size = buf.len() - size_off - mem::size_of::<i32>();

        T::write_i32(&mut buf[size_off..], message_set_size as i32);

        Ok(())
    }

    fn encode_message<T: ByteOrder>(&self,
                                    message: Message,
                                    offset: Offset,
                                    buf: &mut BytesMut)
                                    -> Result<()> {
        buf.put_i64::<T>(offset);
        let size_off = buf.len();
        buf.put_i32::<T>(0);
        let crc_off = buf.len();
        buf.put_i32::<T>(0);
        let data_off = buf.len();
        buf.put_i8(self.api_version as i8);
        buf.put_i8((message.compression as i8 & COMPRESSION_CODEC_MASK) |
                   if let Some(MessageTimestamp::LogAppendTime(_)) = message.timestamp {
                       TIMESTAMP_TYPE_MASK
                   } else {
                       0
                   });

        if self.api_version > 0 {
            buf.put_i64::<T>(message.timestamp.unwrap_or_default().value());
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

named_args!(pub parse_message_set(api_version: ApiVersion)<MessageSet>,
    parse_tag!(ParseTag::MessageSet,
        do_parse!(
            messages: length_count!(be_i32, apply!(parse_message, api_version))
         >> (MessageSet {
                messages: messages,
            })
        )
    )
);

named_args!(parse_message(api_version: ApiVersion)<Message>,
    parse_tag!(ParseTag::Message,
        do_parse!(
            offset: be_i64
         >> size: be_i32
         >> data: peek!(take!(size))
         >> _crc: parse_tag!(ParseTag::MessageCrc,
            verify!(be_i32, |checksum: i32| {
                let crc = crc32::checksum_ieee(&data[mem::size_of::<i32>()..]);

                if crc != checksum as u32 {
                    trace!("message checksum mismatched, expected={}, current={}", crc, checksum as u32);
                }

                crc == checksum as u32
            }))
         >> _magic: verify!(be_i8, |v: i8| v as ApiVersion == api_version)
         >> attrs: be_i8
         >> timestamp: cond!(api_version > 0, be_i64)
         >> key: parse_opt_bytes
         >> value: parse_opt_bytes
         >> (Message {
                offset: offset,
                timestamp: timestamp.map(|ts| if (attrs & TIMESTAMP_TYPE_MASK) == 0 {
                    MessageTimestamp::CreateTime(ts)
                }else {
                    MessageTimestamp::LogAppendTime(ts)
                }),
                compression: Compression::from(attrs & COMPRESSION_CODEC_MASK),
                key: key,
                value: value,
            })
        )
    )
);

/// This class is used to write new log data in memory, i.e.
pub struct MessageSetBuilder {
    api_version: ApiVersion,
    compression: Compression,
    write_limit: usize,
    written_uncompressed: usize,
    base_offset: Offset,
    last_offset: Option<Offset>,
    base_timestamp: Option<Timestamp>,
    message_set: MessageSet,
}

impl MessageSetBuilder {
    pub fn new(api_version: ApiVersion,
               compression: Compression,
               write_limit: usize,
               base_offset: Offset)
               -> Self {
        MessageSetBuilder {
            api_version: api_version,
            compression: compression,
            write_limit: write_limit,
            written_uncompressed: 0,
            base_offset: base_offset,
            last_offset: None,
            base_timestamp: None,
            message_set: MessageSet { messages: vec![] },
        }
    }

    pub fn api_version(&self) -> ApiVersion {
        self.api_version
    }

    pub fn is_full(&self) -> bool {
        !self.message_set.is_empty() && self.write_limit <= self.estimated_bytes()
    }

    pub fn has_room_for(&self,
                        timestamp: Timestamp,
                        key: Option<&Bytes>,
                        value: Option<&Bytes>)
                        -> bool {
        self.message_set.is_empty() ||
        self.write_limit >= self.estimated_bytes() + self.record_size(timestamp, key, value)
    }

    /// Estimate the written bytes to the underlying byte buffer based on uncompressed written bytes
    fn estimated_bytes(&self) -> usize {
        (self.written_uncompressed as f32 *
         match self.compression {
             Compression::None => 1.0,
             Compression::GZIP | Compression::Snappy | Compression::LZ4 => 0.5,
         } * COMPRESSION_RATE_ESTIMATION_FACTOR) as usize
    }

    fn record_size(&self,
                   _timestamp: Timestamp,
                   key: Option<&Bytes>,
                   value: Option<&Bytes>)
                   -> usize {
        let overhead_size = if self.api_version > 0 {
            RECORD_OVERHEAD_V1
        } else {
            RECORD_OVERHEAD_V0
        };

        overhead_size + key.map_or(0, |b| b.len()) + value.map_or(0, |b| b.len())
    }

    pub fn build(self) -> MessageSet {
        self.message_set
    }

    pub fn next_offset(&self) -> Offset {
        self.last_offset.map_or(self.base_offset, |off| off + 1)
    }

    pub fn push(&mut self,
                timestamp: Timestamp,
                key: Option<Bytes>,
                value: Option<Bytes>)
                -> Result<Offset> {
        let offset = self.next_offset();

        self.push_with_offset(offset, timestamp, key, value)
    }

    pub fn push_with_offset(&mut self,
                            offset: Offset,
                            timestamp: Timestamp,
                            key: Option<Bytes>,
                            value: Option<Bytes>)
                            -> Result<Offset> {
        if let Some(last_offset) = self.last_offset {
            if offset <= last_offset {
                bail!(ErrorKind::IllegalArgument(format!("Illegal offset {} following previous offset {}.", offset, last_offset)))
            }
        }

        if timestamp < 0 {
            bail!(ErrorKind::IllegalArgument(format!("Invalid negative timestamp: {}", timestamp)))
        }

        if !self.has_room_for(timestamp, key.as_ref(), value.as_ref()) {
            bail!(ErrorKind::IllegalArgument("message set is full".to_owned()))
        }

        let record_size = self.record_size(timestamp, key.as_ref(), value.as_ref());
        let relative_offset = offset - self.base_offset;

        self.message_set
            .messages
            .push(Message {
                      offset: relative_offset,
                      timestamp: Some(MessageTimestamp::CreateTime(timestamp)),
                      compression: self.compression,
                      key: key,
                      value: value,
                  });

        self.last_offset = Some(offset);

        if self.base_timestamp.is_none() {
            self.base_timestamp = Some(timestamp);
        }

        self.written_uncompressed += record_size;

        Ok(relative_offset)
    }
}

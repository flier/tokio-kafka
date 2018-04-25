use std::fmt;
use std::mem;
use std::ops::Deref;

use bytes::{BufMut, ByteOrder, Bytes, BytesMut};

use nom::{be_i32, be_i64, be_i8};

use time;

use crc::crc32;

use compression::Compression;
use errors::{ErrorKind, Result};
use protocol::{parse_opt_bytes, ApiVersion, Offset, ParseTag, Record, RecordHeader, Timestamp, WriteExt, ZigZag,
               BYTES_LEN_SIZE, NULL_VARINT_SIZE_BYTES, OFFSET_SIZE, RECORD_ATTRIBUTE_LENGTH, TIMESTAMP_SIZE};

pub const TIMESTAMP_TYPE_MASK: i8 = 0x08;
pub const COMPRESSION_CODEC_MASK: i8 = 0x07;

const MSG_SIZE: usize = 4;
const CRC_SIZE: usize = 4;
const MAGIC_SIZE: usize = 1;
const ATTRIBUTE_SIZE: usize = 1;
const RECORD_HEADER_SIZE: usize = OFFSET_SIZE + MSG_SIZE + CRC_SIZE + MAGIC_SIZE + ATTRIBUTE_SIZE;

const COMPRESSION_RATE_ESTIMATION_FACTOR: f32 = 1.05;

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u8)]
pub enum RecordFormat {
    V0,
    V1,
    V2,
}

/// Message sets
///
/// One structure common to both the produce and fetch requests is the message set format.
/// A message in kafka is a key-value pair with a small amount of associated metadata.
/// A message set is just a sequence of messages with offset and size information.
/// This format happens to be used both for the on-disk storage on the broker and the on-the-wire
/// format.
///
/// `MessageSet` => [Offset `MessageSize` Message]
///   Offset => int64
///   `MessageSize` => int32
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

impl Record for MessageSet {
    fn size(&self, record_format: RecordFormat) -> usize {
        self.messages.iter().map(|message| message.size(record_format)).sum()
    }
}

/// Message format
///
/// v0
/// Message => Crc `MagicByte` Attributes Key Value
///   Crc => int32
///   `MagicByte` => int8
///   Attributes => int8
///   Key => bytes
///   Value => bytes
///
/// v1 (supported since 0.10.0)
/// Message => Crc `MagicByte` Attributes Key Value
///   Crc => int32
///   `MagicByte` => int8
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
    pub headers: Vec<RecordHeader>,
}

impl Record for Message {
    fn size(&self, record_format: RecordFormat) -> usize {
        match record_format {
            RecordFormat::V0 | RecordFormat::V1 => {
                let record_overhead_size = RECORD_HEADER_SIZE + if record_format == RecordFormat::V1 {
                    TIMESTAMP_SIZE
                } else {
                    0
                };
                let key_size = BYTES_LEN_SIZE + self.key.as_ref().map_or(0, |b| b.len());
                let value_size = BYTES_LEN_SIZE + self.value.as_ref().map_or(0, |b| b.len());

                record_overhead_size + key_size + value_size
            }
            RecordFormat::V2 => {
                let record_overhead_size = RECORD_ATTRIBUTE_LENGTH + i32::size_of_varint(self.offset as i32)
                    + i64::size_of_varint(self.timestamp.unwrap_or_default().into());
                let key_size = if let Some(ref key) = self.key {
                    i32::size_of_varint(key.len() as i32) + key.len()
                } else {
                    NULL_VARINT_SIZE_BYTES
                };
                let value_size = if let Some(ref value) = self.value {
                    i32::size_of_varint(value.len() as i32) + value.len()
                } else {
                    NULL_VARINT_SIZE_BYTES
                };
                let headers_size = self.headers.iter().fold(
                    i32::size_of_varint(self.headers.len() as i32),
                    |size, header| {
                        let key = header.key.as_bytes();

                        size + i32::size_of_varint(key.len() as i32) + key.len()
                            + header.value.as_ref().map_or(NULL_VARINT_SIZE_BYTES, |value| {
                                i32::size_of_varint(value.len() as i32) + value.len()
                            })
                    },
                );

                record_overhead_size + key_size + value_size + headers_size
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MessageTimestamp {
    CreateTime(Timestamp),
    LogAppendTime(Timestamp),
}

impl From<MessageTimestamp> for Timestamp {
    fn from(ts: MessageTimestamp) -> Timestamp {
        match ts {
            MessageTimestamp::CreateTime(v) | MessageTimestamp::LogAppendTime(v) => v,
        }
    }
}

impl Default for MessageTimestamp {
    fn default() -> Self {
        let ts = time::now_utc().to_timespec();

        MessageTimestamp::CreateTime(ts.sec * 1_000_000 + Timestamp::from(ts.nsec) / 1000)
    }
}

impl fmt::Display for MessageTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MessageTimestamp::CreateTime(ts) => write!(f, "CreateTime:{}", ts),
            MessageTimestamp::LogAppendTime(ts) => write!(f, "LogAppendTime:{}", ts),
        }
    }
}

pub struct MessageSetEncoder {
    record_format: RecordFormat,
    compression: Option<Compression>,
}

impl MessageSetEncoder {
    pub fn new(record_format: RecordFormat, compression: Option<Compression>) -> Self {
        MessageSetEncoder {
            record_format,
            compression,
        }
    }

    pub fn encode<T: ByteOrder>(&self, message_set: &MessageSet, buf: &mut BytesMut) -> Result<()> {
        let mut offset: Offset = 0;

        buf.reserve(message_set.size(self.record_format));

        for message in &message_set.messages {
            let offset = if self.compression.unwrap_or(message.compression) == Compression::None {
                message.offset
            } else {
                offset = offset.wrapping_add(1);
                offset - 1
            };

            self.encode_message::<T>(message, offset, buf)?;
        }

        Ok(())
    }

    fn encode_message<T: ByteOrder>(&self, message: &Message, offset: Offset, buf: &mut BytesMut) -> Result<()> {
        buf.put_i64::<T>(offset);
        let size_off = buf.len();
        buf.put_i32::<T>(0);
        let crc_off = buf.len();
        buf.put_i32::<T>(0);
        let data_off = buf.len();
        buf.put_i8(self.record_format as i8);
        buf.put_i8(
            (self.compression.unwrap_or(message.compression) as i8 & COMPRESSION_CODEC_MASK)
                | if let Some(MessageTimestamp::LogAppendTime(_)) = message.timestamp {
                    TIMESTAMP_TYPE_MASK
                } else {
                    0
                },
        );

        if self.record_format == RecordFormat::V1 {
            buf.put_i64::<T>(message.timestamp.map(Timestamp::from).unwrap_or_default() as i64);
        }

        buf.put_bytes::<T, _>(message.key.as_ref())?;
        buf.put_bytes::<T, _>(message.value.as_ref())?;

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
            messages: many0!(apply!(parse_message, api_version))
         >> (MessageSet {
                messages,
            })
        )
    )
);

named_args!(parse_message(_api_version: ApiVersion)<Message>,
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
         >> magic: be_i8
         >> attrs: be_i8
         >> timestamp: cond!(magic > 0, be_i64)
         >> key: parse_opt_bytes
         >> value: parse_opt_bytes
         >> ({
            Message {
                offset,
                timestamp: timestamp.map(|ts| if (attrs & TIMESTAMP_TYPE_MASK) == 0 {
                    MessageTimestamp::CreateTime(ts)
                }else {
                    MessageTimestamp::LogAppendTime(ts)
                }),
                compression: Compression::from(attrs & COMPRESSION_CODEC_MASK),
                key,
                value,
                headers: Vec::new(),
            }})
        )
    )
);

/// This class is used to write new log data in memory, i.e.
#[derive(Debug)]
pub struct MessageSetBuilder {
    record_format: RecordFormat,
    compression: Compression,
    write_limit: usize,
    written_uncompressed: usize,
    base_offset: Offset,
    last_offset: Option<Offset>,
    base_timestamp: Option<Timestamp>,
    message_set: MessageSet,
}

impl MessageSetBuilder {
    pub fn new(record_format: RecordFormat, compression: Compression, write_limit: usize, base_offset: Offset) -> Self {
        MessageSetBuilder {
            record_format,
            compression,
            write_limit,
            written_uncompressed: 0,
            base_offset,
            last_offset: None,
            base_timestamp: None,
            message_set: MessageSet { messages: vec![] },
        }
    }

    pub fn record_format(&self) -> RecordFormat {
        self.record_format
    }

    pub fn is_full(&self) -> bool {
        !self.message_set.is_empty() && self.write_limit <= self.estimated_bytes()
    }

    pub fn has_room_for(&self, timestamp: Timestamp, key: Option<&Bytes>, value: Option<&Bytes>) -> bool {
        self.message_set.is_empty()
            || self.write_limit >= self.estimated_bytes() + self.record_size(timestamp, key, value)
    }

    /// Estimate the written bytes to the underlying byte buffer based on uncompressed written
    /// bytes
    fn estimated_bytes(&self) -> usize {
        (self.written_uncompressed as f32 * match self.compression {
            Compression::None => 1.0,
            Compression::GZIP | Compression::Snappy | Compression::LZ4 => 0.5,
        } * COMPRESSION_RATE_ESTIMATION_FACTOR) as usize
    }

    fn record_size(&self, _timestamp: Timestamp, key: Option<&Bytes>, value: Option<&Bytes>) -> usize {
        let record_overhead_size = RECORD_HEADER_SIZE + if self.record_format == RecordFormat::V1 {
            TIMESTAMP_SIZE
        } else {
            0
        };
        let key_size = BYTES_LEN_SIZE + key.map_or(0, |b| b.len());
        let value_size = BYTES_LEN_SIZE + value.map_or(0, |b| b.len());

        record_overhead_size + key_size + value_size
    }

    #[cfg(any(feature = "gzip", feature = "snappy", feature = "lz4"))]
    fn wrap<T: ByteOrder>(&self, compression: Compression) -> Result<MessageSet> {
        let mut buf = BytesMut::with_capacity((self.message_set.size(self.record_format) * 6 / 5).next_power_of_two());
        let encoder = MessageSetEncoder::new(self.record_format, Some(Compression::None));
        encoder.encode::<T>(&self.message_set, &mut buf)?;
        let compressed = compression.compress(self.record_format, &buf)?;
        Ok(MessageSet {
            messages: vec![
                Message {
                    offset: 0,
                    timestamp: Some(MessageTimestamp::default()),
                    compression,
                    key: None,
                    value: Some(Bytes::from(compressed)),
                    headers: Vec::new(),
                },
            ],
        })
    }

    pub fn build<T: ByteOrder>(self) -> Result<MessageSet> {
        match self.compression {
            #[cfg(feature = "gzip")]
            Compression::GZIP => self.wrap::<T>(Compression::GZIP),
            #[cfg(feature = "snappy")]
            Compression::Snappy => self.wrap::<T>(Compression::Snappy),
            #[cfg(feature = "lz4")]
            Compression::LZ4 => self.wrap::<T>(Compression::LZ4),
            Compression::None => Ok(self.message_set),
        }
    }

    pub fn next_offset(&self) -> Offset {
        self.last_offset.map_or(self.base_offset, |off| off + 1)
    }

    pub fn push(
        &mut self,
        timestamp: Timestamp,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<RecordHeader>,
    ) -> Result<Offset> {
        let offset = self.next_offset();

        self.push_with_offset(offset, timestamp, key, value, headers)
    }

    pub fn push_with_offset(
        &mut self,
        offset: Offset,
        timestamp: Timestamp,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<RecordHeader>,
    ) -> Result<Offset> {
        if let Some(last_offset) = self.last_offset {
            if offset <= last_offset {
                bail!(ErrorKind::IllegalArgument(format!(
                    "offset {} following previous offset {}.",
                    offset, last_offset
                )))
            }
        }

        if timestamp < 0 {
            bail!(ErrorKind::IllegalArgument(format!("negative timestamp: {}", timestamp)))
        }

        if !self.has_room_for(timestamp, key.as_ref(), value.as_ref()) {
            bail!(ErrorKind::IllegalArgument("message set is full".to_owned()))
        }

        let record_size = self.record_size(timestamp, key.as_ref(), value.as_ref());
        let relative_offset = offset - self.base_offset;

        self.message_set.messages.push(Message {
            offset: relative_offset,
            timestamp: Some(MessageTimestamp::CreateTime(timestamp)),
            compression: self.compression,
            key,
            value,
            headers,
        });

        self.last_offset = Some(offset);

        if self.base_timestamp.is_none() {
            self.base_timestamp = Some(timestamp);
        }

        self.written_uncompressed += record_size;

        Ok(relative_offset)
    }
}

#[cfg(test)]
mod tests {
    use nom::IResult;

    use super::*;
    use protocol::*;

    #[test]
    fn parse_empty_message_set() {
        assert_eq!(
            parse_message_set(&[][..], 0),
            IResult::Done(&[][..], MessageSet { messages: vec![] })
        );
    }

    #[test]
    fn parse_message_set_v0() {
        let data = vec![
            /* messages: [Message] */ 0, 0, 0, 0, 0, 0, 0, 0 /* offset */, 0, 0, 0, 22 /* size */, 197,
            70, 142, 169 /* crc */, 0 /* magic */, 8 /* attributes */, 0, 0, 0, 3, b'k', b'e',
            b'y' /* key */, 0, 0, 0, 5, b'v', b'a', b'l', b'u', b'e' /* value */,
        ];

        let message_set = MessageSet {
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
        };

        let res = parse_message_set(&data[..], 0);

        display_parse_error::<_>(&data[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], message_set));
    }

    #[test]
    fn parse_message_set_v1() {
        let data = vec![
            /* messages: [Message] */ 0, 0, 0, 0, 0, 0, 0, 0 /* offset */, 0, 0, 0, 30 /* size */, 206,
            63, 210, 11 /* crc */, 1 /* magic */, 8 /* attributes */, 0, 0, 0, 0, 0, 0, 1,
            200 /* timestamp */, 0, 0, 0, 3, b'k', b'e', b'y' /* key */, 0, 0, 0, 5, b'v', b'a', b'l', b'u',
            b'e' /* value */,
        ];

        let message_set = MessageSet {
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
        };

        let res = parse_message_set(&data[..], 1);

        display_parse_error::<_>(&data[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], message_set));
    }
}

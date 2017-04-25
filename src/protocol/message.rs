use bytes::{Bytes, BytesMut, BufMut, ByteOrder};

use time;

use crc::crc32;

use errors::Result;
use compression::Compression;
use protocol::WriteExt;

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
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub timestamp: Option<i64>,
}

impl Message {
    pub fn encode<T: ByteOrder>(self,
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

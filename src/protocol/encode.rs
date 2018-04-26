use std::i16;
use std::i32;
use std::str;

use bytes::{BufMut, ByteOrder, BytesMut};

use errors::{ErrorKind, Result};
use protocol::VarIntExt;

pub const STR_LEN_SIZE: usize = 2;
pub const BYTES_LEN_SIZE: usize = 4;
pub const ARRAY_LEN_SIZE: usize = 4;
pub const REPLICA_ID_SIZE: usize = 4;
pub const PARTITION_ID_SIZE: usize = 4;
pub const TIMESTAMP_SIZE: usize = 8;
pub const OFFSET_SIZE: usize = 8;

pub trait Encodable {
    fn encode<T: ByteOrder>(&self, buf: &mut BytesMut) -> Result<()>;
}

pub trait WriteExt: BufMut + Sized {
    fn put_str<T: ByteOrder, S: AsRef<str>>(&mut self, s: Option<S>) -> Result<()> {
        match s.as_ref() {
            Some(v) if v.as_ref().len() > i16::MAX as usize => {
                bail!(ErrorKind::EncodeError("string exceeds the maximum size."))
            }
            Some(v) => {
                self.put_i16::<T>(v.as_ref().len() as i16);

                if !v.as_ref().is_empty() {
                    self.put_slice(v.as_ref().as_bytes());
                }
            }
            _ => {
                self.put_i16::<T>(-1);
            }
        }

        Ok(())
    }

    fn put_bytes<T: ByteOrder, D: AsRef<[u8]>>(&mut self, d: Option<D>) -> Result<()> {
        match d.as_ref() {
            Some(v) if v.as_ref().len() > i32::MAX as usize => {
                bail!(ErrorKind::EncodeError("bytes exceeds the maximum size."))
            }
            Some(v) => {
                self.put_i32::<T>(v.as_ref().len() as i32);

                if !v.as_ref().is_empty() {
                    self.put_slice(v.as_ref());
                }
            }
            _ => {
                self.put_i32::<T>(-1);
            }
        }

        Ok(())
    }

    fn put_varbytes<D: AsRef<[u8]>>(&mut self, d: Option<D>) -> Result<()> {
        match d.as_ref() {
            Some(v) if v.as_ref().len() > i32::MAX as usize => {
                bail!(ErrorKind::EncodeError("bytes exceeds the maximum size."))
            }
            Some(v) => {
                self.put_vari32(v.as_ref().len() as i32);

                if !v.as_ref().is_empty() {
                    self.put_slice(v.as_ref());
                }

                Ok(())
            }
            _ => {
                self.put_vari32(-1);
                Ok(())
            }
        }
    }

    fn put_array<T, E, F>(&mut self, items: &[E], mut callback: F) -> Result<()>
    where
        T: ByteOrder,
        F: FnMut(&mut Self, &E) -> Result<()>,
    {
        if items.len() > i32::MAX as usize {
            bail!(ErrorKind::EncodeError("array exceeds the maximum size."))
        }

        self.put_i32::<T>(items.len() as i32);

        for item in items {
            callback(self, item)?;
        }

        Ok(())
    }

    fn put_vari32(&mut self, value: i32) {
        value.put_varint(self);
    }

    fn put_vari64(&mut self, value: i64) {
        value.put_varint(self);
    }
}

impl<T: BufMut> WriteExt for T {}

#[cfg(test)]
mod tests {
    use std::iter::repeat;
    use std::slice;

    use bytes::BigEndian;

    use super::*;

    #[test]
    fn nullable_str() {
        let mut buf = vec![];

        // write empty nullable string
        buf.put_str::<BigEndian, _>(Some("")).unwrap();

        assert_eq!(buf.as_slice(), &[0, 0]);

        buf.clear();

        // write null of nullable string
        buf.put_str::<BigEndian, String>(None).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255]);

        buf.clear();

        // write nullable string
        buf.put_str::<BigEndian, _>(Some("test")).unwrap();

        assert_eq!(buf.as_slice(), &[0, 4, 116, 101, 115, 116]);

        buf.clear();

        // write encoded nullable string
        buf.put_str::<BigEndian, _>(Some("测试")).unwrap();

        assert_eq!(buf.as_slice(), &[0, 6, 230, 181, 139, 232, 175, 149]);

        buf.clear();

        // write too long nullable string
        let s = repeat(20).take(i16::MAX as usize + 1).collect::<Vec<u8>>();

        assert!(
            buf.put_str::<BigEndian, _>(Some(String::from_utf8(s).unwrap()))
                .err()
                .is_some()
        );
    }

    #[test]
    fn nullable_bytes() {
        let mut buf = vec![];

        // write empty nullable bytes
        buf.put_bytes::<BigEndian, _>(Some(&b""[..])).unwrap();

        assert_eq!(buf.as_slice(), &[0, 0, 0, 0]);

        buf.clear();

        // write null of nullable bytes
        buf.put_bytes::<BigEndian, &[u8]>(None).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255, 255, 255]);

        buf.clear();

        // write nullable bytes
        buf.put_bytes::<BigEndian, _>(Some(&b"test"[..])).unwrap();

        assert_eq!(buf.as_slice(), &[0, 0, 0, 4, 116, 101, 115, 116]);

        buf.clear();

        // write too long nullable bytes
        let s = unsafe { slice::from_raw_parts(buf.as_ptr(), i32::MAX as usize + 1) };

        assert!(buf.put_bytes::<BigEndian, _>(Some(s)).err().is_some());
    }
}

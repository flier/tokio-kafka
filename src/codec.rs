use bytes::{BufMut, ByteOrder};

use errors::{ErrorKind, Result};

pub trait Encodable {
    fn encode<T: ByteOrder, B: BufMut>(&self, buf: B) -> Result<()>;
}

pub trait NullableString {
    fn as_slice(&self) -> Option<&[u8]>;
}

impl<'a> NullableString for &'a str {
    fn as_slice(&self) -> Option<&[u8]> {
        Some(self.as_bytes())
    }
}

impl<'a> NullableString for Option<&'a str> {
    fn as_slice(&self) -> Option<&[u8]> {
        self.map(|s| s.as_bytes())
    }
}

pub trait NullableBytes {
    fn as_slice(&self) -> Option<&[u8]>;
}

impl<'a> NullableBytes for &'a [u8] {
    fn as_slice(&self) -> Option<&[u8]> {
        Some(self)
    }
}

impl<'a> NullableBytes for Option<&'a [u8]> {
    fn as_slice(&self) -> Option<&[u8]> {
        *self
    }
}

pub trait WriteExt: BufMut {
    fn put_str<T: ByteOrder, S: NullableString>(&mut self, s: S) -> Result<()> {
        match s.as_slice() {
            Some(s) if s.len() > i16::max_value() as usize => bail!(ErrorKind::CodecError("String exceeds the maximum size.")),
            Some(s) if s.len() > 0 => {
                self.put_i16::<T>(s.len() as i16);
                self.put_slice(s);
                Ok(())
            }
            _ => {
                self.put_i16::<T>(-1);
                Ok(())
            }
        }
    }

    fn put_bytes<T: ByteOrder, D: NullableBytes>(&mut self, d: D) -> Result<()> {
        match d.as_slice() {
            Some(s) if s.len() > i32::max_value() as usize => bail!(ErrorKind::CodecError("Bytes exceeds the maximum size.")),
            Some(s) if s.len() > 0 => {
                self.put_i32::<T>(s.len() as i32);
                self.put_slice(s);
                Ok(())
            }
            _ => {
                self.put_i32::<T>(-1);
                Ok(())
            }
        }
    }

    fn put_array<T: ByteOrder, E: Encodable>(&mut self, items: &[E]) -> Result<()> {
        if items.len() > i32::max_value() as usize {
            bail!(ErrorKind::CodecError("Array exceeds the maximum size."))
        } else {
            self.put_i32::<T>(items.len() as i32);

            for item in items {
                self.put_item::<T, _>(item)?;
            }

            Ok(())
        }
    }

    fn put_item<T: ByteOrder, E: Encodable>(&mut self, item: &E) -> Result<()> {
        item.encode::<T, _>(self)
    }
}

impl<T: BufMut> WriteExt for T {}

#[cfg(test)]
mod tests {
    use std::str;
    use std::slice;
    use std::iter::repeat;

    use bytes::BigEndian;

    use super::*;

    #[test]
    fn nullable_str() {
        let mut buf = vec![];

        // write empty nullable string
        buf.put_str::<BigEndian, _>("").unwrap();

        assert_eq!(buf.as_slice(), &[255, 255]);

        buf.clear();

        // write null of nullable string
        buf.put_str::<BigEndian, _>(None).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255]);

        buf.clear();

        // write nullable string
        buf.put_str::<BigEndian, _>("test").unwrap();

        assert_eq!(buf.as_slice(), &[0, 4, 116, 101, 115, 116]);

        buf.clear();

        // write encoded nullable string
        buf.put_str::<BigEndian, _>("测试").unwrap();

        assert_eq!(buf.as_slice(), &[0, 6, 230, 181, 139, 232, 175, 149]);

        buf.clear();

        // write too long nullable string
        let s = repeat(20)
            .take(i16::max_value() as usize + 1)
            .collect::<Vec<u8>>();

        assert!(buf.put_str::<BigEndian, _>(str::from_utf8(&s).unwrap())
                    .err()
                    .is_some());
    }

    #[test]
    fn nullable_bytes() {
        let mut buf = vec![];

        // write empty nullable bytes
        buf.put_bytes::<BigEndian, _>(&b""[..]).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255, 255, 255]);

        buf.clear();

        // write null of nullable bytes
        buf.put_bytes::<BigEndian, _>(None).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255, 255, 255]);

        buf.clear();

        // write nullable bytes
        buf.put_bytes::<BigEndian, _>(&b"test"[..]).unwrap();

        assert_eq!(buf.as_slice(), &[0, 0, 0, 4, 116, 101, 115, 116]);

        buf.clear();

        // write too long nullable bytes
        let s = unsafe { slice::from_raw_parts(buf.as_ptr(), i32::max_value() as usize + 1) };

        assert!(buf.put_bytes::<BigEndian, _>(s).err().is_some());
    }
}

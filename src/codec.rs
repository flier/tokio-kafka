use bytes::{BufMut, ByteOrder};

use errors::{ErrorKind, Result};

pub trait Encodable {
    fn encode<T: ByteOrder, B: BufMut>(self, buf: B) -> Result<()>;
}

pub trait WriteExt: BufMut {
    fn put_str<T: ByteOrder, S: AsRef<str>>(&mut self, s: Option<S>) -> Result<()> {
        match s.as_ref() {
            Some(v) if v.as_ref().len() > i16::max_value() as usize => {
                bail!(ErrorKind::CodecError("String exceeds the maximum size."))
            }
            Some(v) if v.as_ref().len() > 0 => {
                self.put_i16::<T>(v.as_ref().len() as i16);
                self.put_slice(v.as_ref().as_bytes());
                Ok(())
            }
            _ => {
                self.put_i16::<T>(-1);
                Ok(())
            }
        }
    }

    fn put_bytes<T: ByteOrder, D: AsRef<[u8]>>(&mut self, d: Option<D>) -> Result<()> {
        match d.as_ref() {
            Some(v) if v.as_ref().len() > i32::max_value() as usize => {
                bail!(ErrorKind::CodecError("Bytes exceeds the maximum size."))
            }
            Some(v) if v.as_ref().len() > 0 => {
                self.put_i32::<T>(v.as_ref().len() as i32);
                self.put_slice(v.as_ref());
                Ok(())
            }
            _ => {
                self.put_i32::<T>(-1);
                Ok(())
            }
        }
    }

    fn put_array<T, E, F>(&mut self, items: Vec<E>, mut callback: F) -> Result<()>
        where T: ByteOrder,
              F: FnMut(&mut Self, E) -> Result<()>
    {
        if items.len() > i32::max_value() as usize {
            bail!(ErrorKind::CodecError("Array exceeds the maximum size."))
        } else {
            self.put_i32::<T>(items.len() as i32);

            for item in items {
                callback(self, item)?;
            }

            Ok(())
        }
    }

    fn put_item<T: ByteOrder, E: Encodable>(&mut self, item: E) -> Result<()> {
        item.encode::<T, _>(self)
    }
}

impl<T: BufMut> WriteExt for T {}

lazy_static! {
    pub static ref CODEPAGE_HEX: Vec<char> = (0_u32..256)
        .map(|c| if 0x20 <= c && c <= 0x7E {
                ::std::char::from_u32(c).unwrap()
            } else {
                '.'
            })
        .collect();
}

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
        buf.put_str::<BigEndian, _>(Some("")).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255]);

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
        let s = repeat(20)
            .take(i16::max_value() as usize + 1)
            .collect::<Vec<u8>>();

        assert!(buf.put_str::<BigEndian, _>(Some(String::from_utf8(s).unwrap()))
                    .err()
                    .is_some());
    }

    #[test]
    fn nullable_bytes() {
        let mut buf = vec![];

        // write empty nullable bytes
        buf.put_bytes::<BigEndian, _>(Some(&b""[..])).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255, 255, 255]);

        buf.clear();

        // write null of nullable bytes
        buf.put_bytes::<BigEndian, &[u8]>(None).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255, 255, 255]);

        buf.clear();

        // write nullable bytes
        buf.put_bytes::<BigEndian, _>(Some(&b"test"[..]))
            .unwrap();

        assert_eq!(buf.as_slice(), &[0, 0, 0, 4, 116, 101, 115, 116]);

        buf.clear();

        // write too long nullable bytes
        let s = unsafe { slice::from_raw_parts(buf.as_ptr(), i32::max_value() as usize + 1) };

        assert!(buf.put_bytes::<BigEndian, _>(Some(s)).err().is_some());
    }
}

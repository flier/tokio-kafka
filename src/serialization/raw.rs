use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::slice;

use bytes::{Buf, BufMut, Bytes};
use bytes::buf::FromBuf;

use errors::{Error, ErrorKind, Result};
use serialization::{Deserializer, Serializer};

/// Serialize type to it's raw data
#[derive(Debug, Default)]
pub struct RawSerializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Clone for RawSerializer<T> {
    fn clone(&self) -> Self {
        RawSerializer { phantom: PhantomData }
    }
}

impl<T> Serializer for RawSerializer<T> {
    type Item = T;
    type Error = Error;

    fn serialize_to<M: BufMut>(
        &self,
        _topic_name: &str,
        data: Self::Item,
        buf: &mut M,
    ) -> Result<()> {
        buf.put_slice(unsafe {
            slice::from_raw_parts(&data as *const T as *const u8, mem::size_of::<T>())
        });

        Ok(())
    }

    fn serialize(&self, _topic_name: &str, data: Self::Item) -> Result<Bytes> {
        Ok(Bytes::from_buf(unsafe {
            slice::from_raw_parts(&data as *const T as *const u8, mem::size_of::<T>())
        }))
    }
}

/// Deserialize type from it's raw data
#[derive(Debug, Default)]
pub struct RawDeserializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Clone for RawDeserializer<T> {
    fn clone(&self) -> Self {
        RawDeserializer { phantom: PhantomData }
    }
}

impl<T> Deserializer for RawDeserializer<T> {
    type Item = T;
    type Error = Error;

    fn deserialize_to<B: Buf>(
        &self,
        _topic_name: &str,
        buf: &mut B,
        data: &mut Self::Item,
    ) -> Result<()> {
        let len = mem::size_of::<T>();

        if buf.remaining() < len {
            bail!(ErrorKind::ParseError(
                "serialized data too small".to_owned(),
            ));
        }

        *data = unsafe { ptr::read(buf.bytes()[..len].as_ptr() as *const T) };

        buf.advance(len);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use serialization::Serializer;

    #[test]
    fn test_seraizlie() {
        let serializer = RawSerializer::default();
        let mut buf = Vec::new();
        let v: u32 = 0x12345678;
        let data = vec![0x78, 0x56, 0x34, 0x12];

        serializer.serialize_to("topic", v, &mut buf).unwrap();

        assert_eq!(buf, data);

        assert_eq!(
            serializer.serialize("topic", v).unwrap(),
            Bytes::from(data.clone())
        );
    }

    #[test]
    fn test_deserialize() {
        let deserializer = RawDeserializer::default();
        let mut cur = Cursor::new(vec![0x78, 0x56, 0x34, 0x12]);
        let mut v = 0u32;

        deserializer
            .deserialize_to("topic", &mut cur, &mut v)
            .unwrap();

        assert_eq!(cur.position(), 4);
        assert_eq!(v, 0x12345678);

        cur.set_position(0);

        assert_eq!(deserializer.deserialize("topic", &mut cur).unwrap(), v);
    }
}

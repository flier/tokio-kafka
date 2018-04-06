use std::marker::PhantomData;

use bytes::buf::FromBuf;
use bytes::{Buf, BufMut, Bytes, IntoBuf};

use errors::{Error, ErrorKind, Result};
use serialization::{Deserializer, Serializer};

/// Serialize `Buf` like type to it's raw bytes
#[derive(Clone, Debug, Default)]
pub struct BytesSerializer<T> {
    phantom: PhantomData<T>,
}

impl<T, B> Serializer for BytesSerializer<T>
where
    T: IntoBuf<Buf = B>,
    B: Buf,
{
    type Item = T;
    type Error = Error;

    fn serialize_to<M: BufMut>(&self, _topic_name: &str, data: Self::Item, buf: &mut M) -> Result<()> {
        buf.put(data.into_buf());
        Ok(())
    }

    fn serialize(&self, _topic_name: &str, data: Self::Item) -> Result<Bytes> {
        Ok(Bytes::from_buf(data.into_buf()))
    }
}

/// Deserialize `Buf` like type from it's raw bytes
#[derive(Clone, Debug, Default)]
pub struct BytesDeserializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Deserializer for BytesDeserializer<T>
where
    T: BufMut,
{
    type Item = T;
    type Error = Error;

    fn deserialize_to<B: Buf>(&self, _topic_name: &str, buf: &mut B, data: &mut Self::Item) -> Result<()> {
        let len = buf.remaining();
        if len > data.remaining_mut() {
            bail!(ErrorKind::EncodeError("buffer too small"));
        }
        data.put_slice(buf.bytes());
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
        let serializer = BytesSerializer::default();
        let mut buf = Vec::new();
        let data = Vec::from("data");

        serializer.serialize_to("topic", data.as_slice(), &mut buf).unwrap();

        assert_eq!(&buf, &data);

        assert_eq!(
            serializer.serialize("topic", data.as_slice()).unwrap(),
            Bytes::from(data)
        );
    }

    #[test]
    fn test_deserialize() {
        let deserializer = BytesDeserializer::default();
        let data = Vec::from("data");
        let mut cur = Cursor::new(data.clone());
        let mut buf = Vec::new();

        deserializer.deserialize_to("topic", &mut cur, &mut buf).unwrap();

        assert_eq!(cur.position(), 4);
        assert_eq!(&buf, &data);

        cur.set_position(0);

        assert_eq!(deserializer.deserialize("topic", &mut cur).unwrap(), data);
    }
}

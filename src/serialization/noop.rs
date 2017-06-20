use std::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes};

use errors::{Error, Result};
use serialization::{Deserializer, Serializer};

/// Serialize type to nothing
#[derive(Debug, Default)]
pub struct NoopSerializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Clone for NoopSerializer<T> {
    fn clone(&self) -> Self {
        NoopSerializer { phantom: PhantomData }
    }
}

impl<T> Serializer for NoopSerializer<T> {
    type Item = T;
    type Error = Error;

    fn serialize_to<B: BufMut>(
        &self,
        _topic_name: &str,
        _data: Self::Item,
        _buf: &mut B,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize(&self, _topic_name: &str, _data: Self::Item) -> Result<Bytes> {
        Ok(Bytes::new())
    }
}

/// Deserialize type from nothing
#[derive(Debug, Default)]
pub struct NoopDeserializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Clone for NoopDeserializer<T> {
    fn clone(&self) -> Self {
        NoopDeserializer { phantom: PhantomData }
    }
}

impl<T> Deserializer for NoopDeserializer<T> {
    type Item = T;
    type Error = Error;

    fn deserialize_to<B: Buf>(
        &self,
        _topic_name: &str,
        buf: &mut B,
        _data: &mut Self::Item,
    ) -> Result<()> {
        let len = buf.remaining();
        buf.advance(len);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_seraizlie() {
        let serializer = NoopSerializer::default();
        let mut buf = Vec::new();

        serializer.serialize_to("topic", "data", &mut buf).unwrap();

        assert!(buf.is_empty());

        let data = serializer.serialize("topic", "data").unwrap();

        assert!(data.is_empty());
    }

    #[test]
    fn test_deserialize() {
        let deserializer = NoopDeserializer::default();
        let mut cur = Cursor::new(Vec::from("data"));
        let mut s = "";

        deserializer
            .deserialize_to("topic", &mut cur, &mut s)
            .unwrap();

        assert_eq!(cur.position(), 4);
        assert!(s.is_empty());

        assert!(
            deserializer
                .deserialize("topic", &mut cur)
                .unwrap()
                .is_empty()
        );
    }
}

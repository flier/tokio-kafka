use std::marker::PhantomData;
use std::str;

use bytes::{Buf, BufMut};

use errors::{Error, Result};
use serialization::{Deserializer, Serializer};

/// Serialize `String` with UTF-8 encoding
#[derive(Debug, Default)]
pub struct StrSerializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Clone for StrSerializer<T> {
    fn clone(&self) -> Self {
        StrSerializer { phantom: PhantomData }
    }
}

impl<T> Serializer for StrSerializer<T>
where
    T: AsRef<str>,
{
    type Item = T;
    type Error = Error;

    fn serialize_to<B: BufMut>(
        &self,
        _topic_name: &str,
        data: Self::Item,
        buf: &mut B,
    ) -> Result<()> {
        buf.put_slice(data.as_ref().as_bytes());
        Ok(())
    }
}

/// Deserialize `String` as UTF-8 encoding
#[derive(Debug, Default)]
pub struct StrDeserializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Clone for StrDeserializer<T> {
    fn clone(&self) -> Self {
        StrDeserializer { phantom: PhantomData }
    }
}

impl Deserializer for StrDeserializer<String> {
    type Item = String;
    type Error = Error;

    fn deserialize_to<B: Buf>(
        &self,
        _topic_name: &str,
        buf: &mut B,
        data: &mut Self::Item,
    ) -> Result<()> {
        let len = buf.remaining();
        *data = str::from_utf8(buf.bytes())?.to_owned();
        buf.advance(len);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_seraizlie() {
        let serializer = StrSerializer::default();
        let mut buf = Vec::new();
        let data = vec![230, 181, 139, 232, 175, 149];

        serializer
            .serialize_to("topic", "测试", &mut buf)
            .unwrap();

        assert_eq!(&buf, &data);

        assert_eq!(
            serializer.serialize("topic", "测试").unwrap(),
            Bytes::from(data)
        );
    }

    #[test]
    fn test_deserialize() {
        let deserializer = StrDeserializer::default();
        let data = vec![230, 181, 139, 232, 175, 149];
        let mut cur = Cursor::new(data.clone());
        let mut s = String::new();

        deserializer
            .deserialize_to("topic", &mut cur, &mut s)
            .unwrap();

        assert_eq!(cur.position(), 6);
        assert_eq!(s, "测试");

        cur.set_position(0);

        assert_eq!(
            deserializer.deserialize("topic", &mut cur).unwrap(),
            "测试"
        );
    }
}

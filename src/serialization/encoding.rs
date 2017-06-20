use std::marker::PhantomData;

use bytes::{Buf, BufMut};

use encoding::{ByteWriter, DecoderTrap, EncoderTrap, Encoding};

use errors::{Error, Result};
use serialization::{Deserializer, Serializer};

struct BufWriter<B>(B)
where
    B: BufMut;

impl<B> ByteWriter for BufWriter<B>
where
    B: BufMut,
{
    fn write_byte(&mut self, b: u8) {
        self.0.put_u8(b)
    }

    fn write_bytes(&mut self, v: &[u8]) {
        self.0.put_slice(v)
    }
}

/// Serialize `String` base on the special encoding
#[derive(Debug)]
pub struct StrEncodingSerializer<E, T> {
    encoding: E,
    phantom: PhantomData<T>,
}

impl<E, T> StrEncodingSerializer<E, T> {
    pub fn new(encoding: E) -> Self {
        StrEncodingSerializer {
            encoding: encoding,
            phantom: PhantomData,
        }
    }
}

impl<E, T> Clone for StrEncodingSerializer<E, T>
where
    E: Encoding + Clone,
    T: AsRef<str>,
{
    fn clone(&self) -> Self {
        StrEncodingSerializer {
            encoding: self.encoding.clone(),
            phantom: PhantomData,
        }
    }
}

impl<E, T> Serializer for StrEncodingSerializer<E, T>
where
    E: Encoding + Clone,
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
        let mut w = BufWriter(buf);

        self.encoding.encode_to(
            data.as_ref(),
            EncoderTrap::Strict,
            &mut w,
        )?;

        Ok(())
    }
}

/// Deserialize `String` base on the special encoding
#[derive(Debug)]
pub struct StrEncodingDeserializer<E, T> {
    encoding: E,
    phantom: PhantomData<T>,
}

impl<E, T> StrEncodingDeserializer<E, T> {
    pub fn new(encoding: E) -> Self {
        StrEncodingDeserializer {
            encoding: encoding,
            phantom: PhantomData,
        }
    }
}

impl<E, T> Clone for StrEncodingDeserializer<E, T>
where
    E: Encoding + Clone,
    T: BufMut,
{
    fn clone(&self) -> Self {
        StrEncodingDeserializer {
            encoding: self.encoding.clone(),
            phantom: PhantomData,
        }
    }
}

impl<E, T> Deserializer for StrEncodingDeserializer<E, T>
where
    E: Encoding + Clone,
    T: BufMut,
{
    type Item = T;
    type Error = Error;

    fn deserialize_to<B: Buf>(
        &self,
        _topic_name: &str,
        buf: &mut B,
        data: &mut Self::Item,
    ) -> Result<()> {
        let len = buf.remaining();
        data.put_slice(
            self.encoding
                .decode(buf.bytes(), DecoderTrap::Strict)?
                .as_bytes(),
        );
        buf.advance(len);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;

    use encoding::codec::simpchinese::GB18030_ENCODING;

    use super::*;

    #[test]
    fn test_seraizlie() {
        let serializer = StrEncodingSerializer::new(GB18030_ENCODING);
        let mut buf = Vec::new();
        let data = vec![178, 226, 202, 212];

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
        let deserializer = StrEncodingDeserializer::new(GB18030_ENCODING);
        let data = vec![178, 226, 202, 212];
        let mut cur = Cursor::new(data.clone());
        let mut buf = Vec::new();

        deserializer
            .deserialize_to("topic", &mut cur, &mut buf)
            .unwrap();

        assert_eq!(cur.position(), 4);
        assert_eq!(buf.as_slice(), "测试".as_bytes());

        cur.set_position(0);

        assert_eq!(
            deserializer.deserialize("topic", &mut cur).unwrap(),
            "测试".as_bytes()
        );
    }
}

use std::marker::PhantomData;

use bytes::{Buf, BufMut};

use encoding::{ByteWriter, DecoderTrap, EncoderTrap, Encoding};

use errors::{Error, Result};
use serialization::{Deserializer, Serializer};

struct BufWriter<B>(B) where B: BufMut;

impl<B> ByteWriter for BufWriter<B>
    where B: BufMut
{
    fn write_byte(&mut self, b: u8) {
        self.0.put_u8(b)
    }

    fn write_bytes(&mut self, v: &[u8]) {
        self.0.put_slice(v)
    }
}

/// Serialize `String` base on the special encoding
#[derive(Clone, Debug)]
pub struct StrEncodingSerializer<E, T> {
    encoding: E,
    phantom: PhantomData<T>,
}

impl<E, T> StrEncodingSerializer<E, T>
    where E: Encoding
{
    pub fn new(encoding: E) -> Self {
        StrEncodingSerializer {
            encoding: encoding,
            phantom: PhantomData,
        }
    }
}

impl<E, T> Serializer for StrEncodingSerializer<E, T>
    where E: Encoding,
          T: AsRef<str>
{
    type Item = T;
    type Error = Error;

    fn serialize_to<B: BufMut>(&self,
                               _topic_name: &str,
                               data: Self::Item,
                               buf: &mut B)
                               -> Result<()> {
        let mut w = BufWriter(buf);

        self.encoding
            .encode_to(data.as_ref(), EncoderTrap::Strict, &mut w)?;

        Ok(())
    }
}

/// Deserialize `String` base on the special encoding
#[derive(Clone, Debug)]
pub struct StrEncodingDeserializer<E, T> {
    encoding: E,
    phantom: PhantomData<T>,
}

impl<E, T> Deserializer for StrEncodingDeserializer<E, T>
    where E: Encoding,
          T: BufMut
{
    type Item = T;
    type Error = Error;

    fn deserialize_from<B: Buf>(&self,
                                _topic_name: &str,
                                buf: &mut B,
                                data: &mut Self::Item)
                                -> Result<()> {
        data.put_slice(self.encoding
                           .decode(buf.bytes(), DecoderTrap::Strict)?
                           .as_bytes());

        Ok(())
    }
}

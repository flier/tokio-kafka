use std::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes, IntoBuf};
use bytes::buf::FromBuf;

use errors::{Error, Result};
use serialization::{Deserializer, Serializer};

/// Serialize `Buf` like type to it's raw bytes
#[derive(Clone, Debug, Default)]
pub struct BytesSerializer<T> {
    phantom: PhantomData<T>,
}

impl<T, B> Serializer for BytesSerializer<T>
    where T: IntoBuf<Buf = B>,
          B: Buf
{
    type Item = T;
    type Error = Error;

    fn serialize_to<M: BufMut>(&self,
                               _topic_name: &str,
                               data: Self::Item,
                               buf: &mut M)
                               -> Result<()> {
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
    where T: BufMut
{
    type Item = T;
    type Error = Error;

    fn deserialize_from<B: Buf>(&self,
                                _topic_name: &str,
                                buf: &mut B,
                                data: &mut Self::Item)
                                -> Result<()> {
        data.put_slice(buf.bytes());

        Ok(())
    }
}

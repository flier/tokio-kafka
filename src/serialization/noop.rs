use std::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes};

use errors::{Error, Result};
use serialization::{Deserializer, Serializer};

/// Serialize type to nothing
#[derive(Clone, Debug, Default)]
pub struct NoopSerializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Serializer for NoopSerializer<T> {
    type Item = T;
    type Error = Error;

    fn serialize_to<B: BufMut>(&self,
                               _topic_name: &str,
                               _data: Self::Item,
                               _buf: &mut B)
                               -> Result<()> {
        Ok(())
    }

    fn serialize(&self, _topic_name: &str, _data: Self::Item) -> Result<Bytes> {
        Ok(Bytes::new())
    }
}

/// Deserialize type from nothing
#[derive(Clone, Debug, Default)]
pub struct NoopDeserializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Deserializer for NoopDeserializer<T> {
    type Item = T;
    type Error = Error;

    fn deserialize_from<B: Buf>(&self,
                                _topic_name: &str,
                                _buf: &mut B,
                                _data: &mut Self::Item)
                                -> Result<()> {
        Ok(())
    }
}

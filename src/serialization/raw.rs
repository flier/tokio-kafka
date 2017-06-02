use std::mem;
use std::ptr;
use std::slice;
use std::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes};
use bytes::buf::FromBuf;

use errors::{Error, ErrorKind, Result};
use serialization::{Deserializer, Serializer};

/// Serialize type to it's raw data
#[derive(Clone, Debug, Default)]
pub struct RawSerializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Serializer for RawSerializer<T> {
    type Item = T;
    type Error = Error;

    fn serialize_to<M: BufMut>(&self,
                               _topic_name: &str,
                               data: Self::Item,
                               buf: &mut M)
                               -> Result<()> {
        buf.put_slice(unsafe {
                          slice::from_raw_parts(&data as *const T as *const u8, mem::size_of::<T>())
                      });

        Ok(())
    }

    fn serialize(&self, _topic_name: &str, data: Self::Item) -> Result<Bytes> {
        Ok(Bytes::from_buf(unsafe {
                               slice::from_raw_parts(&data as *const T as *const u8,
                                                     mem::size_of::<T>())
                           }))
    }
}

/// Deserialize type from it's raw data
#[derive(Clone, Debug, Default)]
pub struct RawDeserializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Deserializer for RawDeserializer<T> {
    type Item = T;
    type Error = Error;

    fn deserialize_from<B: Buf>(&self,
                                _topic_name: &str,
                                buf: &mut B,
                                data: &mut Self::Item)
                                -> Result<()> {
        let len = mem::size_of::<T>();

        if buf.remaining() < len {
            bail!(ErrorKind::ParseError("serialized data too small".to_owned()));
        }

        *data = unsafe { ptr::read(buf.bytes()[..len].as_ptr() as *const T) };

        buf.advance(len);

        Ok(())
    }
}

use std::mem;
use std::slice;
use std::result;
use std::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes, IntoBuf};
use bytes::buf::FromBuf;

use encoding::{ByteWriter, EncoderTrap, Encoding};

use errors::{Error, Result};

pub trait Serializer {
    type Item;
    type Error;

    fn serialize_to<B: BufMut>(&self,
                               topic_name: &str,
                               data: Self::Item,
                               buf: &mut B)
                               -> result::Result<(), Error>;

    fn serialize(&self, topic_name: &str, data: Self::Item) -> result::Result<Bytes, Error> {
        let mut buf = Vec::with_capacity(16);
        self.serialize_to(topic_name, data, &mut buf)?;
        Ok(Bytes::from_buf(buf))
    }
}

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

    fn serialize(&self, _topic_name: &str, _data: Self::Item) -> result::Result<Bytes, Error> {
        Ok(Bytes::new())
    }
}

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

    fn serialize(&self, _topic_name: &str, data: Self::Item) -> result::Result<Bytes, Error> {
        Ok(Bytes::from_buf(unsafe {
                               slice::from_raw_parts(&data as *const T as *const u8,
                                                     mem::size_of::<T>())
                           }))
    }
}

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

    fn serialize(&self, _topic_name: &str, data: Self::Item) -> result::Result<Bytes, Error> {
        Ok(Bytes::from_buf(data.into_buf()))
    }
}

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

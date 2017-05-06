use std::result;
use std::marker::PhantomData;

use bytes::BufMut;

use encoding::{ByteWriter, EncoderTrap, Encoding};

use errors::{Error, Result};

pub trait Serializer {
    type Item;
    type Error;

    fn serialize<B: BufMut>(&self,
                            topic: &str,
                            data: &Self::Item,
                            buf: &mut B)
                            -> result::Result<(), Error>;
}

#[derive(Clone, Debug, Default)]
pub struct NoopSerializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Serializer for NoopSerializer<T> {
    type Item = T;
    type Error = Error;

    fn serialize<B: BufMut>(&self, _topic: &str, _data: &Self::Item, _buf: &mut B) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct StrSerializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Serializer for StrSerializer<T>
    where T: AsRef<str>
{
    type Item = T;
    type Error = Error;

    fn serialize<B: BufMut>(&self, _topic: &str, data: &Self::Item, buf: &mut B) -> Result<()> {
        buf.put_slice(data.as_ref().as_bytes());

        Ok(())
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

    fn serialize<B: BufMut>(&self, _topic: &str, data: &Self::Item, buf: &mut B) -> Result<()> {
        let mut w = BufWriter(buf);

        self.encoding
            .encode_to(data.as_ref(), EncoderTrap::Strict, &mut w)?;

        Ok(())
    }
}

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
                            data: Self::Item,
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

    fn serialize<B: BufMut>(&self, _topic: &str, _data: T, _buf: &mut B) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct StrSerializer<'a> {
    phantom: PhantomData<&'a u8>,
}

impl<'a> Serializer for StrSerializer<'a> {
    type Item = &'a str;
    type Error = Error;

    fn serialize<B: BufMut>(&self, _topic: &str, data: Self::Item, buf: &mut B) -> Result<()> {
        buf.put_slice(data.as_bytes());

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct StringSerializer {}

impl Serializer for StringSerializer {
    type Item = String;
    type Error = Error;

    fn serialize<B: BufMut>(&self, _topic: &str, data: Self::Item, buf: &mut B) -> Result<()> {
        buf.put_slice(data.as_bytes());

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
pub struct StrEncodingSerializer<'a, E> {
    encoding: E,
    phantom: PhantomData<&'a u8>,
}

impl<'a, E> StrEncodingSerializer<'a, E>
    where E: Encoding
{
    pub fn new(encoding: E) -> Self {
        StrEncodingSerializer {
            encoding: encoding,
            phantom: PhantomData,
        }
    }
}

impl<'a, E> Serializer for StrEncodingSerializer<'a, E>
    where E: Encoding
{
    type Item = &'a str;
    type Error = Error;

    fn serialize<B: BufMut>(&self, _topic: &str, data: Self::Item, buf: &mut B) -> Result<()> {
        let mut w = BufWriter(buf);

        self.encoding
            .encode_to(data, EncoderTrap::Strict, &mut w)?;

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct StringEncodingSerializer<E> {
    encoding: E,
}

impl<E> StringEncodingSerializer<E>
    where E: Encoding
{
    pub fn new(encoding: E) -> Self {
        StringEncodingSerializer { encoding: encoding }
    }
}

impl<E> Serializer for StringEncodingSerializer<E>
    where E: Encoding
{
    type Item = String;
    type Error = Error;

    fn serialize<B: BufMut>(&self, _topic: &str, data: Self::Item, buf: &mut B) -> Result<()> {
        let mut w = BufWriter(buf);

        self.encoding
            .encode_to(&data, EncoderTrap::Strict, &mut w)?;

        Ok(())
    }
}

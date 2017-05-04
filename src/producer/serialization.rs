use bytes::BufMut;
use std::marker::PhantomData;

pub trait Serializer {
    type Item;

    fn serialize<B: BufMut>(topic: &str, data: Self::Item, buf: &mut B);
}

#[derive(Clone, Debug, Default)]
pub struct NoopSerializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Serializer for NoopSerializer<T> {
    type Item = T;

    fn serialize<B: BufMut>(_topic: &str, _data: T, _buf: &mut B) {}
}

#[derive(Clone, Debug, Default)]
pub struct StrSerializer<'a> {
    encoding: Option<String>,
    phantom: PhantomData<&'a u8>,
}

impl<'a> Serializer for StrSerializer<'a> {
    type Item = &'a str;

    fn serialize<B: BufMut>(_topic: &str, data: Self::Item, buf: &mut B) {
        buf.put_slice(data.as_bytes())
    }
}

#[derive(Clone, Debug, Default)]
pub struct StringSerializer {
    encoding: Option<String>,
}

impl Serializer for StringSerializer {
    type Item = String;

    fn serialize<B: BufMut>(_topic: &str, data: Self::Item, buf: &mut B) {
        buf.put_slice(data.as_bytes())
    }
}

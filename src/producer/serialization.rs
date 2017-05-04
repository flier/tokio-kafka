use bytes::BufMut;
use std::marker::PhantomData;

pub trait Serialize {}

impl<'a> Serialize for &'a str {}
impl Serialize for String {}

pub trait Serializer {
    type Item: Serialize;

    fn serialize<B: BufMut>(topic: &str, data: Self::Item, buf: &mut B);
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

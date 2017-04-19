use bytes::{BufMut, ByteOrder};

use nom::be_i32;

use errors::Result;
use codec::{Encodable, WriteExt};

#[derive(Debug)]
pub struct RequestHeader<'a> {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<&'a str>,
}

impl<'a> Encodable for RequestHeader<'a> {
    fn encode<T: ByteOrder, B: BufMut>(&self, mut buf: B) -> Result<()> {
        buf.put_i16::<T>(self.api_key);
        buf.put_i16::<T>(self.api_version);
        buf.put_i32::<T>(self.correlation_id);
        buf.put_str::<T, _>(self.client_id)
    }
}

#[derive(Debug)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

named!(pub parse_response_header<ResponseHeader>,
    do_parse!(
        correlation_id: be_i32
     >> (
            ResponseHeader {
                correlation_id: correlation_id,
            }
        )
    )
);

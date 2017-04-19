use bytes::{BufMut, ByteOrder};

use nom::be_i32;

use errors::Result;
use codec::{Encodable, WriteExt};

#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug, PartialEq)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

named!(pub parse_response_header<ResponseHeader>,
    do_parse!(
        correlation_id: be_i32
     >> (ResponseHeader {
            correlation_id: correlation_id,
        })
    )
);


#[cfg(test)]
mod tests {
    use bytes::BigEndian;

    use super::*;
    use protocol::*;

    #[test]
    fn test_request_header() {
        let hdr = RequestHeader {
            api_key: ApiKeys::Fetch as i16,
            api_version: 2,
            correlation_id: 123,
            client_id: Some("test"),
        };

        let mut buf = vec![];

        buf.put_item::<BigEndian, _>(&hdr).unwrap();

        assert_eq!(&buf[..],
                   &[0, 1,            // api_key
                               0, 2,            // api_version
                               0, 0, 0, 123,    // correlation_id
                               0, 4, 116, 101, 115, 116]);

        let bytes = &[0, 0, 0, 123];
        let res = parse_response_header(bytes);

        assert!(res.is_done());

        let (remaning, hdr) = res.unwrap();

        assert_eq!(remaning, b"");
        assert_eq!(hdr.correlation_id, 123);
    }
}
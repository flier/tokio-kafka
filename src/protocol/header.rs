use std::borrow::Cow;
use bytes::{BufMut, ByteOrder, BytesMut};

use nom::be_i32;

use errors::Result;
use protocol::{ApiKey, ApiVersion, CorrelationId, Encodable, ParseTag, WriteExt};

#[derive(Clone, Debug, PartialEq)]
pub struct RequestHeader<'a> {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub client_id: Option<Cow<'a, str>>,
}

impl<'a> Encodable for RequestHeader<'a> {
    fn encode<T: ByteOrder>(self, buf: &mut BytesMut) -> Result<()> {
        buf.put_i16::<T>(self.api_key);
        buf.put_i16::<T>(self.api_version);
        buf.put_i32::<T>(self.correlation_id);
        buf.put_str::<T, _>(self.client_id)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResponseHeader {
    pub correlation_id: CorrelationId,
}

named!(pub parse_response_header<ResponseHeader>,
    parse_tag!(ParseTag::ResponseHeader, do_parse!(
        correlation_id: be_i32
     >> (ResponseHeader {
            correlation_id: correlation_id,
        })
    ))
);

#[cfg(test)]
mod tests {

    use super::*;
    use bytes::BigEndian;
    use protocol::*;

    #[test]
    fn test_request_header() {
        let hdr = RequestHeader {
            api_key: ApiKeys::Fetch as ApiKey,
            api_version: 2,
            correlation_id: 123,
            client_id: Some("test".into()),
        };

        let mut buf = BytesMut::with_capacity(64);

        hdr.encode::<BigEndian>(&mut buf).unwrap();

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
use std::ops::Deref;
use std::iter::FromIterator;

use bytes::BufMut;

use nom::{IResult, be_i16, be_i32};

use errors::Result;
use protocol::{parse_response_header, ApiKeys, ApiVersion, Encodable, ErrorCode, ParseTag, Request, RequestHeader,
               ResponseHeader};

#[derive(Clone, Debug, PartialEq)]
pub struct ApiVersionsRequest<'a> {
    pub header: RequestHeader<'a>,
}

impl<'a> Request for ApiVersionsRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version)
    }
}

impl<'a> Encodable for ApiVersionsRequest<'a> {
    fn encode<T: BufMut>(&self, dst: &mut T) -> Result<()> {
        self.header.encode(dst)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApiVersionsResponse {
    pub header: ResponseHeader,
    /// Error code.
    pub error_code: ErrorCode,
    /// API versions supported by the broker.
    pub api_versions: Vec<UsableApiVersion>,
}

/// API versions supported by the broker.
#[derive(Clone, Debug, PartialEq)]
pub struct UsableApiVersion {
    /// API key.
    pub api_key: ApiKeys,
    /// Minimum supported version.
    pub min_version: ApiVersion,
    /// Maximum supported version.
    pub max_version: ApiVersion,
}

/// Maintains node api versions for access outside of Client
#[derive(Clone, Debug)]
pub struct UsableApiVersions(Vec<UsableApiVersion>);

impl Deref for UsableApiVersions {
    type Target = [UsableApiVersion];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl UsableApiVersions {
    /// Find API version for the given ApiKeys
    pub fn find(&self, api_key: ApiKeys) -> Option<&UsableApiVersion> {
        self.0.iter().find(|v| v.api_key == api_key)
    }
}

impl FromIterator<UsableApiVersion> for UsableApiVersions {
    fn from_iter<T: IntoIterator<Item = UsableApiVersion>>(iter: T) -> Self {
        UsableApiVersions(iter.into_iter().collect())
    }
}

impl ApiVersionsResponse {
    pub fn parse(buf: &[u8]) -> IResult<&[u8], Self> {
        parse_api_versions_response(buf)
    }
}

named!(
    parse_api_versions_response<ApiVersionsResponse>,
    parse_tag!(
        ParseTag::ApiVersionsResponse,
        do_parse!(
            header: parse_response_header >> error_code: be_i16
                >> api_versions: length_count!(be_i32, parse_api_version) >> (ApiVersionsResponse {
                header,
                error_code,
                api_versions,
            })
        )
    )
);

named!(
    parse_api_version<UsableApiVersion>,
    parse_tag!(
        ParseTag::ApiVersion,
        do_parse!(
            api_key: be_i16 >> min_version: be_i16 >> max_version: be_i16 >> (UsableApiVersion {
                api_key: ApiKeys::from(api_key),
                min_version,
                max_version,
            })
        )
    )
);

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use nom::IResult;

    use super::*;
    use protocol::*;

    lazy_static!{
        static ref TEST_REQUEST_DATA: Vec<u8> = vec![
            // ApiVersionsRequest
                // RequestHeader
                0, 18,                              // api_key
                0, 0,                               // api_version
                0, 0, 0, 123,                       // correlation_id
                0, 6, 99, 108, 105, 101, 110, 116,  // client_id
        ];

        static ref TEST_RESPONSE_DATA: Vec<u8> = vec![
            // ResponseHeader
            0, 0, 0, 123,   // correlation_id
            0, 0,           // error_code
            // api_versions: [ApiVersion]
            0, 0, 0, 1,
                0, 1,       // api_key
                0, 2,       // min_version
                0, 3,       // max_version
        ];

        static ref TEST_RESPONSE: ApiVersionsResponse = ApiVersionsResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 0,
            api_versions: vec![UsableApiVersion {
                api_key: ApiKeys::Fetch,
                min_version: 2,
                max_version: 3,
            }],
        };
    }

    #[test]
    fn test_encode_api_versions_request() {
        let req = ApiVersionsRequest {
            header: RequestHeader {
                api_key: ApiKeys::ApiVersions as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
        };

        let mut buf = BytesMut::with_capacity(128);

        req.encode(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &TEST_REQUEST_DATA[..]);
    }

    #[test]
    fn test_parse_api_versions_response() {
        assert_eq!(
            parse_api_versions_response(TEST_RESPONSE_DATA.as_slice()),
            IResult::Done(&[][..], TEST_RESPONSE.clone())
        );
    }
}

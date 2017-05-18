use std::borrow::Cow;

use bytes::{ByteOrder, BytesMut};

use nom::{be_i16, be_i32};

use errors::Result;
use protocol::{ApiVersion, Encodable, ErrorCode, ParseTag, Record, RequestHeader, ResponseHeader,
               STR_LEN_SIZE, WriteExt, parse_response_header, parse_string};

#[derive(Clone, Debug, PartialEq)]
pub struct GroupCoordinatorRequest<'a> {
    pub header: RequestHeader<'a>,
    pub group_id: Cow<'a, str>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupCoordinatorResponse {
    pub header: ResponseHeader,
    pub error_code: ErrorCode,
    pub coordinator_id: i32,
    pub coordinator_host: String,
    pub coordinator_port: i32,
}

impl<'a> Record for GroupCoordinatorRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + STR_LEN_SIZE + self.group_id.len()
    }
}

impl<'a> Encodable for GroupCoordinatorRequest<'a> {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        self.header.encode::<T>(dst)?;

        dst.put_str::<T, _>(Some(self.group_id.as_ref()))
    }
}

named!(pub parse_group_corordinator_response<GroupCoordinatorResponse>,
    parse_tag!(ParseTag::GroupCoordinatorResponse,
        do_parse!(
            header: parse_response_header
         >> error_code: be_i16
         >> coordinator_id: be_i32
         >> coordinator_host: parse_string
         >> coordinator_port: be_i32
         >> (GroupCoordinatorResponse {
                header: header,
                error_code: error_code,
                coordinator_id: coordinator_id,
                coordinator_host: coordinator_host,
                coordinator_port: coordinator_port,
            })
        )
    )
);

#[cfg(test)]
mod tests {
    use bytes::BigEndian;

    use nom::IResult;

    use protocol::{ApiKey, ApiKeys};

    use super::*;

    lazy_static!{
        static ref TEST_REQUEST_DATA: Vec<u8> = vec![
            // ApiVersionsRequest
                // RequestHeader
                0, 10,                              // api_key
                0, 0,                               // api_version
                0, 0, 0, 123,                       // correlation_id
                0, 6, 99, 108, 105, 101, 110, 116,  // client_id

            0, 8, 99, 111, 110, 115, 117, 109, 101, 114 // group_id
        ];

        static ref TEST_RESPONSE_DATA: Vec<u8> = vec![
            // ResponseHeader
            0, 0, 0, 123,   // correlation_id

            0, 1,                                   // error_code
            0, 0, 0, 2,                             // coordinator_id
            0, 9, 108, 111, 99, 97, 108, 104, 111, 115, 116,
                                                    // coordinator_host
            0, 0, 0, 3,                             // coordinator_port
        ];

        static ref TEST_RESPONSE: GroupCoordinatorResponse = GroupCoordinatorResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 1,
            coordinator_id: 2,
            coordinator_host: "localhost".to_owned(),
            coordinator_port: 3,
        };
    }

    #[test]
    fn test_group_coordinator_request() {
        let req = GroupCoordinatorRequest {
            header: RequestHeader {
                api_key: ApiKeys::GroupCoordinator as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            group_id: "consumer".into(),
        };

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &TEST_REQUEST_DATA[..]);
    }

    #[test]
    fn test_parse_group_corordinator_response() {
        assert_eq!(parse_group_corordinator_response(TEST_RESPONSE_DATA.as_slice()),
                   IResult::Done(&[][..], TEST_RESPONSE.clone()));
    }
}

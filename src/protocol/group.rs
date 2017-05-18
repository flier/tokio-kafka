use std::borrow::Cow;

use bytes::{BufMut, ByteOrder, Bytes, BytesMut};

use nom::{be_i16, be_i32};

use errors::Result;
use protocol::{ARRAY_LEN_SIZE, ApiVersion, BYTES_LEN_SIZE, Encodable, ErrorCode, ParseTag, Record,
               RequestHeader, ResponseHeader, STR_LEN_SIZE, WriteExt, parse_bytes,
               parse_response_header, parse_string};

const SESSION_TIMEOUT_SIZE: usize = 4;
const REBALANCE_TIMEOUT_SIZE: usize = 4;
const GROUP_GENERATION_ID_SIZE: usize = 4;

#[derive(Clone, Debug, PartialEq)]
pub struct GroupCoordinatorRequest<'a> {
    pub header: RequestHeader<'a>,
    /// The unique group id.
    pub group_id: Cow<'a, str>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupCoordinatorResponse {
    pub header: ResponseHeader,
    /// Error code.
    pub error_code: ErrorCode,
    /// The broker id.
    pub coordinator_id: i32,
    /// The hostname of the broker.
    pub coordinator_host: String,
    /// The port on which the broker accepts requests.
    pub coordinator_port: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinGroupRequest<'a> {
    pub header: RequestHeader<'a>,
    /// The unique group id.
    pub group_id: Cow<'a, str>,
    /// The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms.
    pub session_timeout: i32,
    /// The maximum time that the coordinator will wait for each member to rejoin when rebalancing the group
    pub rebalance_timeout: i32,
    /// The assigned consumer id or an empty string for a new consumer.
    pub member_id: Cow<'a, str>,
    /// Unique name for class of protocols implemented by group
    pub protocol_type: Cow<'a, str>,
    /// List of protocols that the member supports
    pub protocols: Vec<JoinGroupProtocol<'a>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinGroupProtocol<'a> {
    pub protocol_name: Cow<'a, str>,

    pub protocol_metadata: Cow<'a, [u8]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinGroupResponse {
    pub header: ResponseHeader,
    /// Error code.
    pub error_code: ErrorCode,
    /// The generation of the consumer group.
    pub generation_id: i32,
    /// The group protocol selected by the coordinator
    pub group_protocol: String,
    /// The leader id assigned by the group coordinator.
    pub leader_id: String,
    /// The member id assigned by the group coordinator.
    pub member_id: String,

    pub members: Vec<JoinGroupMember>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinGroupMember {
    pub member_id: String,

    pub member_metadata: Bytes,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HeartbeatRequest<'a> {
    pub header: RequestHeader<'a>,
    /// The unique group id.
    pub group_id: Cow<'a, str>,
    /// The generation of the group.
    pub group_generation_id: i32,
    /// The member id assigned by the group coordinator.
    pub member_id: Cow<'a, str>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HeartbeatResponse {
    pub header: ResponseHeader,
    /// Error code.
    pub error_code: ErrorCode,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeaveGroupRequest<'a> {
    pub header: RequestHeader<'a>,
    /// The unique group id.
    pub group_id: Cow<'a, str>,
    /// The member id assigned by the group coordinator.
    pub member_id: Cow<'a, str>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeaveGroupResponse {
    pub header: ResponseHeader,
    /// Error code.
    pub error_code: ErrorCode,
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

impl<'a> Record for JoinGroupRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) +
        {
            STR_LEN_SIZE + self.group_id.len()
        } + SESSION_TIMEOUT_SIZE +
        if api_version > 0 {
            REBALANCE_TIMEOUT_SIZE
        } else {
            0
        } +
        {
            STR_LEN_SIZE + self.member_id.len()
        } +
        {
            STR_LEN_SIZE + self.protocol_type.len()
        } +
        self.protocols
            .iter()
            .fold(ARRAY_LEN_SIZE, |size, protocol| {
                size +
                {
                    STR_LEN_SIZE + protocol.protocol_name.len()
                } +
                {
                    BYTES_LEN_SIZE + protocol.protocol_metadata.len()
                }
            })
    }
}

impl<'a> Encodable for JoinGroupRequest<'a> {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        let api_version = self.header.api_version;

        self.header.encode::<T>(dst)?;

        dst.put_str::<T, _>(Some(self.group_id.as_ref()))?;
        dst.put_i32::<T>(self.session_timeout);
        if api_version > 0 {
            dst.put_i32::<T>(self.rebalance_timeout);
        }
        dst.put_str::<T, _>(Some(self.member_id.as_ref()))?;
        dst.put_str::<T, _>(Some(self.protocol_type.as_ref()))?;

        dst.put_array::<T, _, _>(&self.protocols, |buf, protocol| {
            buf.put_str::<T, _>(Some(protocol.protocol_name.as_ref()))?;
            buf.put_bytes::<T, _>(Some(protocol.protocol_metadata.as_ref()))
        })
    }
}

impl<'a> Record for HeartbeatRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) +
        {
            STR_LEN_SIZE + self.group_id.len()
        } + GROUP_GENERATION_ID_SIZE +
        {
            STR_LEN_SIZE + self.member_id.len()
        }
    }
}

impl<'a> Encodable for HeartbeatRequest<'a> {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        self.header.encode::<T>(dst)?;

        dst.put_str::<T, _>(Some(self.group_id.as_ref()))?;
        dst.put_i32::<T>(self.group_generation_id);
        dst.put_str::<T, _>(Some(self.member_id.as_ref()))
    }
}

impl<'a> Record for LeaveGroupRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) +
        {
            STR_LEN_SIZE + self.group_id.len()
        } +
        {
            STR_LEN_SIZE + self.member_id.len()
        }
    }
}

impl<'a> Encodable for LeaveGroupRequest<'a> {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        self.header.encode::<T>(dst)?;

        dst.put_str::<T, _>(Some(self.group_id.as_ref()))?;
        dst.put_str::<T, _>(Some(self.member_id.as_ref()))
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

named!(pub parse_join_group_response<JoinGroupResponse>,
    parse_tag!(ParseTag::JoinGroupResponse,
        do_parse!(
            header: parse_response_header
         >> error_code: be_i16
         >> generation_id: be_i32
         >> group_protocol: parse_string
         >> leader_id: parse_string
         >> member_id: parse_string
         >> members: length_count!(be_i32, parse_group_member)
         >> (JoinGroupResponse {
                header: header,
                error_code: error_code,
                generation_id: generation_id,
                group_protocol: group_protocol,
                leader_id: leader_id,
                member_id: member_id,
                members: members,
            })
        )
    )
);

named!(parse_group_member<JoinGroupMember>,
    parse_tag!(ParseTag::JoinGroupMember,
        do_parse!(
            member_id: parse_string
         >> member_metadata: parse_bytes
         >> (JoinGroupMember {
                member_id: member_id,
                member_metadata: member_metadata,
            })
        )
    )
);

named!(pub parse_heartbeat_response<HeartbeatResponse>,
    parse_tag!(ParseTag::HeartbeatResponse,
        do_parse!(
            header: parse_response_header
         >> error_code: be_i16
         >> (HeartbeatResponse {
                header: header,
                error_code: error_code,
            })
        )
    )
);

named!(pub parse_leave_group_response<LeaveGroupResponse>,
    parse_tag!(ParseTag::LeaveGroupResponse,
        do_parse!(
            header: parse_response_header
         >> error_code: be_i16
         >> (LeaveGroupResponse {
                header: header,
                error_code: error_code,
            })
        )
    )
);

#[cfg(test)]
mod tests {
    use bytes::BigEndian;

    use nom::IResult;

    use protocol::*;

    use super::*;

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

        let data = vec![
            // ApiVersionsRequest
                // RequestHeader
                0, 10,                                      // api_key
                0, 0,                                       // api_version
                0, 0, 0, 123,                               // correlation_id
                0, 6, b'c', b'l', b'i', b'e', b'n', b't',   // client_id

            0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r',   // group_id
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_group_corordinator_response() {
        let data = vec![
            // ResponseHeader
            0, 0, 0, 123,   // correlation_id

            0, 1,                                   // error_code
            0, 0, 0, 2,                             // coordinator_id
            0, 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't',
                                                    // coordinator_host
            0, 0, 0, 3,                             // coordinator_port
        ];

        let res = GroupCoordinatorResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 1,
            coordinator_id: 2,
            coordinator_host: "localhost".to_owned(),
            coordinator_port: 3,
        };

        assert_eq!(parse_group_corordinator_response(data.as_slice()),
                   IResult::Done(&[][..], res));
    }

    #[test]
    fn test_encode_join_group_request_v0() {
        let req = JoinGroupRequest {
            header: RequestHeader {
                api_key: ApiKeys::JoinGroup as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            group_id: "consumer".into(),
            session_timeout: 1,
            rebalance_timeout: 2,
            member_id: "member".into(),
            protocol_type: "protocol".into(),
            protocols: vec![JoinGroupProtocol {
                protocol_name: "protocol".into(),
                protocol_metadata: Cow::Borrowed(b"metadata"),
            }],
        };

        let data = vec![
            // JoinGroupRequest
                // RequestHeader
                0, 11,                                      // api_key
                0, 0,                                       // api_version
                0, 0, 0, 123,                               // correlation_id
                0, 6, b'c', b'l', b'i', b'e', b'n', b't',   // client_id

            0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r',   // group_id
            0, 0, 0, 1,                                             // session_timeout
            0, 6, b'm', b'e', b'm', b'b', b'e', b'r',               // member_id
            0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o', b'l',   // protocol_type

                // protocols: [JoinGroupProtocol]
                0, 0, 0, 1,
                    // JoinGroupProtocol
                    0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o', b'l',       // protocol_name
                    0, 0, 0, 8, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a', // protocol_metadata

        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_encode_join_group_request_v1() {
        let req = JoinGroupRequest {
            header: RequestHeader {
                api_key: ApiKeys::JoinGroup as ApiKey,
                api_version: 1,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            group_id: "consumer".into(),
            session_timeout: 1,
            rebalance_timeout: 2,
            member_id: "member".into(),
            protocol_type: "protocol".into(),
            protocols: vec![JoinGroupProtocol {
                protocol_name: "protocol".into(),
                protocol_metadata: Cow::Borrowed(b"metadata"),
            }],
        };

        let data = vec![
            // JoinGroupRequest
                // RequestHeader
                0, 11,                                      // api_key
                0, 1,                                       // api_version
                0, 0, 0, 123,                               // correlation_id
                0, 6, b'c', b'l', b'i', b'e', b'n', b't',   // client_id

            0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r',   // group_id
            0, 0, 0, 1,                                             // session_timeout
            0, 0, 0, 2,                                             // rebalance_timeout
            0, 6, b'm', b'e', b'm', b'b', b'e', b'r',               // member_id
            0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o', b'l',   // protocol_type

                // protocols: [JoinGroupProtocol]
                0, 0, 0, 1,
                    // JoinGroupProtocol
                    0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o', b'l',       // protocol_name
                    0, 0, 0, 8, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a', // protocol_metadata

        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_join_group_response() {
        let response = JoinGroupResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 1,
            generation_id: 2,
            group_protocol: "protocol".to_owned(),
            leader_id: "leader".to_owned(),
            member_id: "member".to_owned(),
            members: vec![JoinGroupMember {
                member_id: "id".to_owned(),
                member_metadata: Bytes::from(&b"metadata"[..]),
            }],
        };

        let data = vec![
            // ResponseHeader
            0, 0, 0, 123,   // correlation_id

            0, 1,                                                   // error_code
            0, 0, 0, 2,                                             // generation_id
            0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o', b'l',   // group_protocol
            0, 6, b'l', b'e', b'a', b'd', b'e', b'r',               // leader_id
            0, 6, b'm', b'e', b'm', b'b', b'e', b'r',               // member_id
            // members: [JoinGroupMember]
            0, 0, 0, 1,
                // JoinGroupMember
                0, 2, b'i', b'd',                                           // member_id
                0, 0, 0, 8, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a', // member_metadata
        ];

        let res = parse_join_group_response(&data[..]);

        display_parse_error::<_>(&data[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], response));
    }

    #[test]
    fn test_encode_heartbeat_request() {
        let req = HeartbeatRequest {
            header: RequestHeader {
                api_key: ApiKeys::Heartbeat as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            group_id: "consumer".into(),
            group_generation_id: 456,
            member_id: "member".into(),
        };

        let data = vec![
            // ApiVersionsRequest
                // RequestHeader
                0, 12,                                      // api_key
                0, 0,                                       // api_version
                0, 0, 0, 123,                               // correlation_id
                0, 6, b'c', b'l', b'i', b'e', b'n', b't',   // client_id

            0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r',   // group_id
            0, 0, 1, 200,                                           // group_generation_id
            0, 6, b'm', b'e', b'm', b'b', b'e', b'r',               // member_id
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_heartbeat_response() {
        let data = vec![
            // ResponseHeader
            0, 0, 0, 123,   // correlation_id

            0, 1,                                   // error_code
        ];

        let res = HeartbeatResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 1,
        };

        assert_eq!(parse_heartbeat_response(data.as_slice()),
                   IResult::Done(&[][..], res));
    }

    #[test]
    fn test_encode_leave_group_request() {
        let req = LeaveGroupRequest {
            header: RequestHeader {
                api_key: ApiKeys::LeaveGroup as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            group_id: "consumer".into(),
            member_id: "member".into(),
        };

        let data = vec![
            // ApiVersionsRequest
                // RequestHeader
                0, 13,                                      // api_key
                0, 0,                                       // api_version
                0, 0, 0, 123,                               // correlation_id
                0, 6, b'c', b'l', b'i', b'e', b'n', b't',   // client_id

            0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r',   // group_id
            0, 6, b'm', b'e', b'm', b'b', b'e', b'r',               // member_id
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_leave_group_response() {
        let data = vec![
            // ResponseHeader
            0, 0, 0, 123,   // correlation_id

            0, 1,                                   // error_code
        ];

        let res = LeaveGroupResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 1,
        };

        assert_eq!(parse_leave_group_response(data.as_slice()),
                   IResult::Done(&[][..], res));
    }
}

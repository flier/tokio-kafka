use std::borrow::Cow;

use bytes::{BufMut, Bytes};

use nom::{IResult, be_i16, be_i32};

use errors::Result;
use protocol::{parse_bytes, parse_response_header, parse_string, ApiVersion, Encodable, ErrorCode, GenerationId,
               ParseTag, Request, RequestHeader, ResponseHeader, WriteExt, ARRAY_LEN_SIZE, BYTES_LEN_SIZE,
               STR_LEN_SIZE};

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
    /// The coordinator considers the consumer dead if it receives no heartbeat after this timeout
    /// in ms.
    pub session_timeout: i32,
    /// The maximum time that the coordinator will wait for each member to rejoin when rebalancing
    /// the group
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
    pub generation_id: GenerationId,
    /// The group protocol selected by the coordinator
    pub protocol: String,
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
    pub group_generation_id: GenerationId,
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

#[derive(Clone, Debug, PartialEq)]
pub struct SyncGroupRequest<'a> {
    pub header: RequestHeader<'a>,
    /// The unique group id.
    pub group_id: Cow<'a, str>,
    /// The generation of the group.
    pub group_generation_id: GenerationId,
    /// The member id assigned by the group coordinator.
    pub member_id: Cow<'a, str>,

    pub group_assignment: Vec<SyncGroupAssignment<'a>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncGroupAssignment<'a> {
    /// The member id assigned by the group coordinator.
    pub member_id: Cow<'a, str>,

    pub member_assignment: Cow<'a, [u8]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncGroupResponse {
    pub header: ResponseHeader,
    /// Error code.
    pub error_code: ErrorCode,

    pub member_assignment: Bytes,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeGroupsRequest<'a> {
    pub header: RequestHeader<'a>,
    /// List of groupIds to request metadata for (an empty groupId array will return empty group
    /// metadata).
    pub groups: Vec<Cow<'a, str>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeGroupsResponse {
    pub header: ResponseHeader,

    pub groups: Vec<DescribeGroupsGroupStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeGroupsGroupStatus {
    /// Error code.
    pub error_code: ErrorCode,
    /// The unique group id.
    pub group_id: String,
    /// The current state of the group (one of: Dead, Stable, AwaitingSync, or PreparingRebalance,
    /// or empty if there is no active group)
    pub state: String,
    /// The current group protocol type (will be empty if there is no active
    /// group)
    pub protocol_type: String,
    /// The current group protocol (only provided if the group is Stable)
    pub protocol: String,
    /// Current group members (only provided if the group is not Dead)
    pub members: Vec<DescribeGroupsMemberStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeGroupsMemberStatus {
    /// The memberId assigned by the coordinator
    pub member_id: String,
    /// The client id used in the member's latest join group request
    pub client_id: String,
    /// The client host used in the request session corresponding to the
    /// member's join group.
    pub client_host: String,
    /// The metadata corresponding to the current group protocol in use (will only be present if
    /// the group is stable).
    pub member_metadata: Bytes,
    /// The current assignment provided by the group leader (will only be present if the group is
    /// stable).
    pub member_assignment: Bytes,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListGroupsRequest<'a> {
    pub header: RequestHeader<'a>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListGroupsResponse {
    pub header: ResponseHeader,
    /// Error code.
    pub error_code: ErrorCode,

    pub groups: Vec<ListGroupsGroupStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListGroupsGroupStatus {
    /// The unique group id.
    pub group_id: String,
    /// The current group protocol type (will be empty if there is no active
    /// group)
    pub protocol_type: String,
}

impl<'a> Request for GroupCoordinatorRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + STR_LEN_SIZE + self.group_id.len()
    }
}

impl<'a> Encodable for GroupCoordinatorRequest<'a> {
    fn encode<T: BufMut>(&self, dst: &mut T) -> Result<()> {
        self.header.encode(dst)?;

        dst.put_str(Some(self.group_id.as_ref()))
    }
}

impl<'a> Request for JoinGroupRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + { STR_LEN_SIZE + self.group_id.len() } + SESSION_TIMEOUT_SIZE
            + if api_version > 0 { REBALANCE_TIMEOUT_SIZE } else { 0 } + { STR_LEN_SIZE + self.member_id.len() }
            + { STR_LEN_SIZE + self.protocol_type.len() }
            + self.protocols.iter().fold(ARRAY_LEN_SIZE, |size, protocol| {
                size + { STR_LEN_SIZE + protocol.protocol_name.len() } + {
                    BYTES_LEN_SIZE + protocol.protocol_metadata.len()
                }
            })
    }
}

impl<'a> Encodable for JoinGroupRequest<'a> {
    fn encode<T: BufMut>(&self, dst: &mut T) -> Result<()> {
        let api_version = self.header.api_version;

        self.header.encode(dst)?;

        dst.put_str(Some(self.group_id.as_ref()))?;
        dst.put_i32_be(self.session_timeout);
        if api_version > 0 {
            dst.put_i32_be(self.rebalance_timeout);
        }
        dst.put_str(Some(self.member_id.as_ref()))?;
        dst.put_str(Some(self.protocol_type.as_ref()))?;

        dst.put_array(&self.protocols, |buf, protocol| {
            buf.put_str(Some(protocol.protocol_name.as_ref()))?;
            buf.put_bytes(Some(protocol.protocol_metadata.as_ref()))
        })
    }
}

impl<'a> Request for HeartbeatRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + { STR_LEN_SIZE + self.group_id.len() } + GROUP_GENERATION_ID_SIZE + {
            STR_LEN_SIZE + self.member_id.len()
        }
    }
}

impl<'a> Encodable for HeartbeatRequest<'a> {
    fn encode<T: BufMut>(&self, dst: &mut T) -> Result<()> {
        self.header.encode(dst)?;

        dst.put_str(Some(self.group_id.as_ref()))?;
        dst.put_i32_be(self.group_generation_id);
        dst.put_str(Some(self.member_id.as_ref()))
    }
}

impl<'a> Request for LeaveGroupRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + { STR_LEN_SIZE + self.group_id.len() } + { STR_LEN_SIZE + self.member_id.len() }
    }
}

impl<'a> Encodable for LeaveGroupRequest<'a> {
    fn encode<T: BufMut>(&self, dst: &mut T) -> Result<()> {
        self.header.encode(dst)?;

        dst.put_str(Some(self.group_id.as_ref()))?;
        dst.put_str(Some(self.member_id.as_ref()))
    }
}

impl<'a> Request for SyncGroupRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + { STR_LEN_SIZE + self.group_id.len() } + GROUP_GENERATION_ID_SIZE + {
            STR_LEN_SIZE + self.member_id.len()
        } + self.group_assignment.iter().fold(ARRAY_LEN_SIZE, |size, member| {
            size + { STR_LEN_SIZE + member.member_id.len() } + { BYTES_LEN_SIZE + member.member_assignment.len() }
        })
    }
}

impl<'a> Encodable for SyncGroupRequest<'a> {
    fn encode<T: BufMut>(&self, dst: &mut T) -> Result<()> {
        self.header.encode(dst)?;

        dst.put_str(Some(self.group_id.as_ref()))?;
        dst.put_i32_be(self.group_generation_id);
        dst.put_str(Some(self.member_id.as_ref()))?;

        dst.put_array(&self.group_assignment, |buf, assignment| {
            buf.put_str(Some(assignment.member_id.as_ref()))?;
            buf.put_bytes(Some(assignment.member_assignment.as_ref()))
        })
    }
}

impl<'a> Request for DescribeGroupsRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version)
            + self.groups
                .iter()
                .fold(ARRAY_LEN_SIZE, |size, group| size + STR_LEN_SIZE + group.len())
    }
}

impl<'a> Encodable for DescribeGroupsRequest<'a> {
    fn encode<T: BufMut>(&self, dst: &mut T) -> Result<()> {
        self.header.encode(dst)?;

        dst.put_array(&self.groups, |buf, group| buf.put_str(Some(group.as_ref())))
    }
}

impl<'a> Request for ListGroupsRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version)
    }
}

impl<'a> Encodable for ListGroupsRequest<'a> {
    fn encode<T: BufMut>(&self, dst: &mut T) -> Result<()> {
        self.header.encode(dst)
    }
}

impl GroupCoordinatorResponse {
    pub fn parse(buf: &[u8]) -> IResult<&[u8], Self> {
        parse_group_corordinator_response(buf)
    }
}

named!(
    parse_group_corordinator_response<GroupCoordinatorResponse>,
    parse_tag!(
        ParseTag::GroupCoordinatorResponse,
        do_parse!(
            header: parse_response_header >> error_code: be_i16 >> coordinator_id: be_i32
                >> coordinator_host: parse_string >> coordinator_port: be_i32
                >> (GroupCoordinatorResponse {
                    header,
                    error_code,
                    coordinator_id,
                    coordinator_host,
                    coordinator_port,
                })
        )
    )
);

impl JoinGroupResponse {
    pub fn parse(buf: &[u8]) -> IResult<&[u8], Self> {
        parse_join_group_response(buf)
    }
}

named!(
    parse_join_group_response<JoinGroupResponse>,
    parse_tag!(
        ParseTag::JoinGroupResponse,
        do_parse!(
            header: parse_response_header >> error_code: be_i16 >> generation_id: be_i32 >> protocol: parse_string
                >> leader_id: parse_string >> member_id: parse_string
                >> members: length_count!(be_i32, parse_group_member) >> (JoinGroupResponse {
                header,
                error_code,
                generation_id,
                protocol,
                leader_id,
                member_id,
                members,
            })
        )
    )
);

named!(
    parse_group_member<JoinGroupMember>,
    parse_tag!(
        ParseTag::JoinGroupMember,
        do_parse!(
            member_id: parse_string >> member_metadata: parse_bytes >> (JoinGroupMember {
                member_id,
                member_metadata,
            })
        )
    )
);

impl HeartbeatResponse {
    pub fn parse(buf: &[u8]) -> IResult<&[u8], Self> {
        parse_heartbeat_response(buf)
    }
}

named!(
    parse_heartbeat_response<HeartbeatResponse>,
    parse_tag!(
        ParseTag::HeartbeatResponse,
        do_parse!(header: parse_response_header >> error_code: be_i16 >> (HeartbeatResponse { header, error_code }))
    )
);

impl LeaveGroupResponse {
    pub fn parse(buf: &[u8]) -> IResult<&[u8], Self> {
        parse_leave_group_response(buf)
    }
}

named!(
    parse_leave_group_response<LeaveGroupResponse>,
    parse_tag!(
        ParseTag::LeaveGroupResponse,
        do_parse!(header: parse_response_header >> error_code: be_i16 >> (LeaveGroupResponse { header, error_code }))
    )
);

impl SyncGroupResponse {
    pub fn parse(buf: &[u8]) -> IResult<&[u8], Self> {
        parse_sync_group_response(buf)
    }
}

named!(
    parse_sync_group_response<SyncGroupResponse>,
    parse_tag!(
        ParseTag::SyncGroupResponse,
        do_parse!(
            header: parse_response_header >> error_code: be_i16 >> member_assignment: parse_bytes
                >> (SyncGroupResponse {
                    header,
                    error_code,
                    member_assignment,
                })
        )
    )
);

impl DescribeGroupsResponse {
    pub fn parse(buf: &[u8]) -> IResult<&[u8], Self> {
        parse_describe_groups_response(buf)
    }
}

named!(
    parse_describe_groups_response<DescribeGroupsResponse>,
    parse_tag!(
        ParseTag::DescribeGroupsResponse,
        do_parse!(
            header: parse_response_header >> groups: length_count!(be_i32, parse_describe_groups_group_status)
                >> (DescribeGroupsResponse { header, groups })
        )
    )
);

named!(
    parse_describe_groups_group_status<DescribeGroupsGroupStatus>,
    parse_tag!(
        ParseTag::DescribeGroupsGroupStatus,
        do_parse!(
            error_code: be_i16 >> group_id: parse_string >> state: parse_string >> protocol_type: parse_string
                >> protocol: parse_string
                >> members: length_count!(be_i32, parse_describe_groups_member_status)
                >> (DescribeGroupsGroupStatus {
                    error_code,
                    group_id,
                    state,
                    protocol_type,
                    protocol,
                    members,
                })
        )
    )
);

named!(
    parse_describe_groups_member_status<DescribeGroupsMemberStatus>,
    parse_tag!(
        ParseTag::DescribeGroupsMemberStatus,
        do_parse!(
            member_id: parse_string >> client_id: parse_string >> client_host: parse_string
                >> member_metadata: parse_bytes >> member_assignment: parse_bytes
                >> (DescribeGroupsMemberStatus {
                    member_id,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                })
        )
    )
);

impl ListGroupsResponse {
    pub fn parse(buf: &[u8]) -> IResult<&[u8], Self> {
        parse_list_groups_response(buf)
    }
}

named!(
    parse_list_groups_response<ListGroupsResponse>,
    parse_tag!(
        ParseTag::ListGroupsResponse,
        do_parse!(
            header: parse_response_header >> error_code: be_i16
                >> groups: length_count!(be_i32, parse_list_groups_group_status) >> (ListGroupsResponse {
                header,
                error_code,
                groups,
            })
        )
    )
);

named!(
    parse_list_groups_group_status<ListGroupsGroupStatus>,
    parse_tag!(
        ParseTag::ListGroupsGroupStatus,
        do_parse!(
            group_id: parse_string >> protocol_type: parse_string >> (ListGroupsGroupStatus {
                group_id,
                protocol_type,
            })
        )
    )
);

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use nom::IResult;

    use protocol::*;

    use super::*;

    #[test]
    fn test_encode_group_coordinator_request() {
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
            /* ApiVersionsRequest
             * RequestHeader */ 0, 10 /* api_key */, 0,
            0 /* api_version */, 0, 0, 0, 123 /* correlation_id */, 0, 6, b'c', b'l', b'i', b'e', b'n',
            b't' /* client_id */, 0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r' /* group_id */,
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_group_corordinator_response() {
        let data = vec![
            /* ResponseHeader */ 0, 0, 0, 123 /* correlation_id */, 0, 1 /* error_code */, 0, 0, 0,
            2 /* coordinator_id */, 0, 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't',
            /* coordinator_host */ 0, 0, 0, 3 /* coordinator_port */,
        ];

        let res = GroupCoordinatorResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 1,
            coordinator_id: 2,
            coordinator_host: "localhost".to_owned(),
            coordinator_port: 3,
        };

        assert_eq!(
            parse_group_corordinator_response(data.as_slice()),
            IResult::Done(&[][..], res)
        );
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
            protocols: vec![
                JoinGroupProtocol {
                    protocol_name: "protocol".into(),
                    protocol_metadata: Cow::Borrowed(b"metadata"),
                },
            ],
        };

        let data = vec![
            /* JoinGroupRequest
             * RequestHeader */ 0, 11 /* api_key */, 0,
            0 /* api_version */, 0, 0, 0, 123 /* correlation_id */, 0, 6, b'c', b'l', b'i', b'e', b'n',
            b't' /* client_id */, 0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r' /* group_id */, 0, 0,
            0, 1 /* session_timeout */, 0, 6, b'm', b'e', b'm', b'b', b'e', b'r' /* member_id */, 0, 8, b'p',
            b'r', b'o', b't', b'o', b'c', b'o', b'l' /* protocol_type */,
            /* protocols: [JoinGroupProtocol] */ 0, 0, 0, 1, /* JoinGroupProtocol */ 0, 8, b'p', b'r', b'o',
            b't', b'o', b'c', b'o', b'l' /* protocol_name */, 0, 0, 0, 8, b'm', b'e', b't', b'a', b'd', b'a',
            b't', b'a' /* protocol_metadata */,
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode(&mut buf).unwrap();

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
            protocols: vec![
                JoinGroupProtocol {
                    protocol_name: "protocol".into(),
                    protocol_metadata: Cow::Borrowed(b"metadata"),
                },
            ],
        };

        let data = vec![
            /* JoinGroupRequest
             * RequestHeader */ 0, 11 /* api_key */, 0,
            1 /* api_version */, 0, 0, 0, 123 /* correlation_id */, 0, 6, b'c', b'l', b'i', b'e', b'n',
            b't' /* client_id */, 0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r' /* group_id */, 0, 0,
            0, 1 /* session_timeout */, 0, 0, 0, 2 /* rebalance_timeout */, 0, 6, b'm', b'e', b'm', b'b',
            b'e', b'r' /* member_id */, 0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o',
            b'l' /* protocol_type */, /* protocols: [JoinGroupProtocol] */ 0, 0, 0, 1,
            /* JoinGroupProtocol */ 0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o', b'l' /* protocol_name */,
            0, 0, 0, 8, b'm', b'e', b't', b'a', b'd', b'a', b't', b'a' /* protocol_metadata */,
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_join_group_response() {
        let response = JoinGroupResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 1,
            generation_id: 2,
            protocol: "protocol".to_owned(),
            leader_id: "leader".to_owned(),
            member_id: "member".to_owned(),
            members: vec![
                JoinGroupMember {
                    member_id: "id".to_owned(),
                    member_metadata: Bytes::from(&b"metadata"[..]),
                },
            ],
        };

        let data = vec![
            /* ResponseHeader */ 0, 0, 0, 123 /* correlation_id */, 0, 1 /* error_code */, 0, 0, 0,
            2 /* generation_id */, 0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o', b'l' /* protocol */, 0, 6,
            b'l', b'e', b'a', b'd', b'e', b'r' /* leader_id */, 0, 6, b'm', b'e', b'm', b'b', b'e',
            b'r' /* member_id */, /* members: [JoinGroupMember] */ 0, 0, 0, 1, /* JoinGroupMember */ 0,
            2, b'i', b'd' /* member_id */, 0, 0, 0, 8, b'm', b'e', b't', b'a', b'd', b'a', b't',
            b'a' /* member_metadata */,
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
            /* ApiVersionsRequest
             * RequestHeader */ 0, 12 /* api_key */, 0,
            0 /* api_version */, 0, 0, 0, 123 /* correlation_id */, 0, 6, b'c', b'l', b'i', b'e', b'n',
            b't' /* client_id */, 0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r' /* group_id */, 0, 0,
            1, 200 /* group_generation_id */, 0, 6, b'm', b'e', b'm', b'b', b'e', b'r' /* member_id */,
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_heartbeat_response() {
        let data = vec![
            /* ResponseHeader */ 0, 0, 0, 123 /* correlation_id */, 0, 1 /* error_code */
        ];

        let res = HeartbeatResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 1,
        };

        assert_eq!(parse_heartbeat_response(data.as_slice()), IResult::Done(&[][..], res));
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
            /* ApiVersionsRequest
             * RequestHeader */ 0, 13 /* api_key */, 0,
            0 /* api_version */, 0, 0, 0, 123 /* correlation_id */, 0, 6, b'c', b'l', b'i', b'e', b'n',
            b't' /* client_id */, 0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r' /* group_id */, 0, 6,
            b'm', b'e', b'm', b'b', b'e', b'r' /* member_id */,
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_leave_group_response() {
        let data = vec![
            /* ResponseHeader */ 0, 0, 0, 123 /* correlation_id */, 0, 1 /* error_code */
        ];

        let res = LeaveGroupResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 1,
        };

        assert_eq!(parse_leave_group_response(data.as_slice()), IResult::Done(&[][..], res));
    }

    #[test]
    fn test_encode_sync_group_request() {
        let req = SyncGroupRequest {
            header: RequestHeader {
                api_key: ApiKeys::SyncGroup as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            group_id: "consumer".into(),
            group_generation_id: 456,
            member_id: "member".into(),
            group_assignment: vec![
                SyncGroupAssignment {
                    member_id: "member".into(),
                    member_assignment: Cow::Borrowed(b"assignment"),
                },
            ],
        };

        let data = vec![
            /* ApiVersionsRequest
             * RequestHeader */ 0, 14 /* api_key */, 0,
            0 /* api_version */, 0, 0, 0, 123 /* correlation_id */, 0, 6, b'c', b'l', b'i', b'e', b'n',
            b't' /* client_id */, 0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r' /* group_id */, 0, 0,
            1, 200 /* group_generation_id */, 0, 6, b'm', b'e', b'm', b'b', b'e', b'r' /* member_id */,
            /* group_assignment: [SyncGroupAssignment] */ 0, 0, 0, 1, /* SyncGroupAssignment */ 0, 6, b'm',
            b'e', b'm', b'b', b'e', b'r' /* member_id */, 0, 0, 0, 10, b'a', b's', b's', b'i', b'g', b'n', b'm',
            b'e', b'n', b't' /* member_assignment */,
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_sync_group_response() {
        let data = vec![
            /* ResponseHeader */ 0, 0, 0, 123 /* correlation_id */, 0, 1 /* error_code */, 0, 0, 0, 10,
            b'a', b's', b's', b'i', b'g', b'n', b'm', b'e', b'n', b't' /* member_assignment */,
        ];

        let res = SyncGroupResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 1,
            member_assignment: Bytes::from(&b"assignment"[..]),
        };

        assert_eq!(parse_sync_group_response(data.as_slice()), IResult::Done(&[][..], res));
    }

    #[test]
    fn test_encode_describe_groups_request() {
        let req = DescribeGroupsRequest {
            header: RequestHeader {
                api_key: ApiKeys::DescribeGroups as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            groups: vec!["consumer".into()],
        };

        let data = vec![
            /* DescribeGroupsRequest
             * RequestHeader */ 0, 15 /* api_key */, 0,
            0 /* api_version */, 0, 0, 0, 123 /* correlation_id */, 0, 6, b'c', b'l', b'i', b'e', b'n',
            b't' /* client_id */, /* groups[String] */ 0, 0, 0, 1, /* String */ 0, 8, b'c', b'o', b'n',
            b's', b'u', b'm', b'e', b'r' /* group_id */,
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_describe_groups_response() {
        let response = DescribeGroupsResponse {
            header: ResponseHeader { correlation_id: 123 },
            groups: vec![
                DescribeGroupsGroupStatus {
                    error_code: 1,
                    group_id: "consumer".to_owned(),
                    state: "Stable".to_owned(),
                    protocol_type: "type".to_owned(),
                    protocol: "protocol".to_owned(),
                    members: vec![
                        DescribeGroupsMemberStatus {
                            member_id: "member".to_owned(),
                            client_id: "client".to_owned(),
                            client_host: "localhost".to_owned(),
                            member_metadata: Bytes::from(&b"metadata"[..]),
                            member_assignment: Bytes::from(&b"assignment"[..]),
                        },
                    ],
                },
            ],
        };

        let data = vec![
            /* ResponseHeader */ 0, 0, 0, 123 /* correlation_id */,
            /* groups: [DescribeGroupsGroupStatus] */ 0, 0, 0, 1, /* DescribeGroupsGroupStatus */ 0,
            1 /* error_code */, 0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r' /* group_id */, 0, 6,
            b'S', b't', b'a', b'b', b'l', b'e' /* state */, 0, 4, b't', b'y', b'p', b'e' /* protocol_type */,
            0, 8, b'p', b'r', b'o', b't', b'o', b'c', b'o', b'l' /* protocol */,
            /* members: [DescribeGroupsMemberStatus] */ 0, 0, 0, 1, /* DescribeGroupsMemberStatus */ 0, 6,
            b'm', b'e', b'm', b'b', b'e', b'r' /* member_id */, 0, 6, b'c', b'l', b'i', b'e', b'n',
            b't' /* client_id */, 0, 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's',
            b't' /* client_host */, 0, 0, 0, 8, b'm', b'e', b't', b'a', b'd', b'a', b't',
            b'a' /* member_metadata */, 0, 0, 0, 10, b'a', b's', b's', b'i', b'g', b'n', b'm', b'e', b'n',
            b't' /* member_assignment */,
        ];

        let res = parse_describe_groups_response(&data[..]);

        display_parse_error::<_>(&data[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], response));
    }

    #[test]
    fn test_encode_list_groups_request() {
        let req = ListGroupsRequest {
            header: RequestHeader {
                api_key: ApiKeys::ListGroups as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
        };

        let data = vec![
            /* ListGroupsRequest
             * RequestHeader */ 0, 16 /* api_key */, 0,
            0 /* api_version */, 0, 0, 0, 123 /* correlation_id */, 0, 6, b'c', b'l', b'i', b'e', b'n',
            b't' /* client_id */,
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_list_groups_response() {
        let response = ListGroupsResponse {
            header: ResponseHeader { correlation_id: 123 },
            error_code: 1,
            groups: vec![
                ListGroupsGroupStatus {
                    group_id: "consumer".to_owned(),
                    protocol_type: "type".to_owned(),
                },
            ],
        };

        let data = vec![
            /* ResponseHeader */ 0, 0, 0, 123 /* correlation_id */, 0, 1 /* error_code */,
            /* groups: [ListGroupsGroupStatus] */ 0, 0, 0, 1, /* ListGroupsGroupStatus */ 0, 8, b'c', b'o',
            b'n', b's', b'u', b'm', b'e', b'r' /* group_id */, 0, 4, b't', b'y', b'p',
            b'e' /* protocol_type */,
        ];

        let res = parse_list_groups_response(&data[..]);

        display_parse_error::<_>(&data[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], response));
    }

}

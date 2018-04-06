use std::io;

use log::Level::Debug;

use nom::{self, ErrorKind, IResult, Needed};

use protocol::{display_parse_error, ApiKeys, ApiVersion, ApiVersionsResponse,
               DescribeGroupsResponse, FetchResponse, GroupCoordinatorResponse, HeartbeatResponse,
               JoinGroupResponse, LeaveGroupResponse, ListGroupsResponse, ListOffsetResponse,
               MetadataResponse, OffsetCommitResponse, OffsetFetchResponse, ParseTag,
               ProduceResponse, SyncGroupResponse};

#[derive(Clone, Debug, PartialEq)]
pub enum KafkaResponse {
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    ListOffsets(ListOffsetResponse),
    Metadata(MetadataResponse),
    OffsetCommit(OffsetCommitResponse),
    OffsetFetch(OffsetFetchResponse),
    GroupCoordinator(GroupCoordinatorResponse),
    JoinGroup(JoinGroupResponse),
    Heartbeat(HeartbeatResponse),
    LeaveGroup(LeaveGroupResponse),
    SyncGroup(SyncGroupResponse),
    DescribeGroups(DescribeGroupsResponse),
    ListGroups(ListGroupsResponse),
    ApiVersions(ApiVersionsResponse),
}

impl KafkaResponse {
    pub fn api_key(&self) -> ApiKeys {
        match *self {
            KafkaResponse::Produce(_) => ApiKeys::Produce,
            KafkaResponse::Fetch(_) => ApiKeys::Fetch,
            KafkaResponse::ListOffsets(_) => ApiKeys::ListOffsets,
            KafkaResponse::Metadata(_) => ApiKeys::Metadata,
            KafkaResponse::OffsetCommit(_) => ApiKeys::OffsetCommit,
            KafkaResponse::OffsetFetch(_) => ApiKeys::OffsetFetch,
            KafkaResponse::GroupCoordinator(_) => ApiKeys::GroupCoordinator,
            KafkaResponse::JoinGroup(_) => ApiKeys::JoinGroup,
            KafkaResponse::Heartbeat(_) => ApiKeys::Heartbeat,
            KafkaResponse::LeaveGroup(_) => ApiKeys::LeaveGroup,
            KafkaResponse::SyncGroup(_) => ApiKeys::SyncGroup,
            KafkaResponse::DescribeGroups(_) => ApiKeys::DescribeGroups,
            KafkaResponse::ListGroups(_) => ApiKeys::ListGroups,
            KafkaResponse::ApiVersions(_) => ApiKeys::ApiVersions,
        }
    }

    pub fn parse<T: AsRef<[u8]>>(
        src: T,
        api_key: ApiKeys,
        api_version: ApiVersion,
    ) -> io::Result<Option<Self>> {
        let buf = src.as_ref();

        debug!(
            "parsing {:?} response (api_version = {:?}) with {} bytes",
            api_key,
            api_version,
            buf.len(),
        );

        let res = match api_key {
            ApiKeys::Produce => {
                ProduceResponse::parse(buf, api_version).map(KafkaResponse::Produce)
            }
            ApiKeys::Fetch => FetchResponse::parse(buf, api_version).map(KafkaResponse::Fetch),
            ApiKeys::ListOffsets => {
                ListOffsetResponse::parse(buf, api_version).map(KafkaResponse::ListOffsets)
            }
            ApiKeys::Metadata => MetadataResponse::parse(buf).map(KafkaResponse::Metadata),
            ApiKeys::OffsetCommit => {
                OffsetCommitResponse::parse(buf).map(KafkaResponse::OffsetCommit)
            }
            ApiKeys::OffsetFetch => OffsetFetchResponse::parse(buf).map(KafkaResponse::OffsetFetch),
            ApiKeys::GroupCoordinator => {
                GroupCoordinatorResponse::parse(buf).map(KafkaResponse::GroupCoordinator)
            }
            ApiKeys::JoinGroup => JoinGroupResponse::parse(buf).map(KafkaResponse::JoinGroup),
            ApiKeys::Heartbeat => HeartbeatResponse::parse(buf).map(KafkaResponse::Heartbeat),
            ApiKeys::LeaveGroup => LeaveGroupResponse::parse(buf).map(KafkaResponse::LeaveGroup),
            ApiKeys::SyncGroup => SyncGroupResponse::parse(buf).map(KafkaResponse::SyncGroup),
            ApiKeys::DescribeGroups => {
                DescribeGroupsResponse::parse(buf).map(KafkaResponse::DescribeGroups)
            }
            ApiKeys::ListGroups => ListGroupsResponse::parse(buf).map(KafkaResponse::ListGroups),
            ApiKeys::ApiVersions => ApiVersionsResponse::parse(buf).map(KafkaResponse::ApiVersions),
            _ => IResult::Error(nom::Err::Code(ErrorKind::Custom(ParseTag::ApiKey as u32))),
        };

        match res {
            IResult::Done(remaining, res) => {
                debug!("parsed response: {:?}", res);

                if !remaining.is_empty() {
                    warn!("remaining {} bytes not parsed", remaining.len());
                }

                Ok(Some(res))
            }
            IResult::Incomplete(needed) => {
                warn!(
                    "incomplete response, need more {} bytes",
                    if let Needed::Size(size) = needed {
                        size.to_string()
                    } else {
                        "unknown".to_owned()
                    }
                );

                debug!("\n{}", hexdump!(buf));

                Ok(None)
            }
            IResult::Error(err) => {
                if log_enabled!(Debug) {
                    display_parse_error::<KafkaResponse>(&buf[..], IResult::Error(err.clone()));
                }

                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("fail to parse response, {}", err),
                ))
            }
        }
    }
}

use std::io;

use log::LogLevel::Debug;

use nom::{self, ErrorKind, IResult, Needed};

use protocol::{ApiKeys, ApiVersion, ApiVersionsResponse, FetchResponse, GroupCoordinatorResponse,
               HeartbeatResponse, JoinGroupResponse, ListOffsetResponse, MetadataResponse,
               OffsetCommitResponse, OffsetFetchResponse, ParseTag, ProduceResponse,
               display_parse_error, parse_api_versions_response, parse_fetch_response,
               parse_group_corordinator_response, parse_heartbeat_response,
               parse_join_group_response, parse_list_offset_response, parse_metadata_response,
               parse_offset_commit_response, parse_offset_fetch_response, parse_produce_response};

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
            KafkaResponse::ApiVersions(_) => ApiKeys::ApiVersions,
        }
    }

    pub fn parse<T: AsRef<[u8]>>(src: T,
                                 api_key: ApiKeys,
                                 api_version: ApiVersion)
                                 -> io::Result<Option<Self>> {
        let buf = src.as_ref();

        debug!("parsing {} bytes response as {:?} ({:?})",
               buf.len(),
               api_key,
               api_version);

        let res = match api_key {
            ApiKeys::Produce => {
                parse_produce_response(buf, api_version).map(KafkaResponse::Produce)
            }
            ApiKeys::Fetch => parse_fetch_response(buf, api_version).map(KafkaResponse::Fetch),
            ApiKeys::ListOffsets => {
                parse_list_offset_response(buf, api_version).map(KafkaResponse::ListOffsets)
            }
            ApiKeys::Metadata => parse_metadata_response(buf).map(KafkaResponse::Metadata),
            ApiKeys::OffsetCommit => {
                parse_offset_commit_response(buf).map(KafkaResponse::OffsetCommit)
            }
            ApiKeys::OffsetFetch => {
                parse_offset_fetch_response(buf).map(KafkaResponse::OffsetFetch)
            }
            ApiKeys::GroupCoordinator => {
                parse_group_corordinator_response(buf).map(KafkaResponse::GroupCoordinator)
            }
            ApiKeys::JoinGroup => parse_join_group_response(buf).map(KafkaResponse::JoinGroup),
            ApiKeys::Heartbeat => parse_heartbeat_response(buf).map(KafkaResponse::Heartbeat),
            ApiKeys::ApiVersions => {
                parse_api_versions_response(buf).map(KafkaResponse::ApiVersions)
            }
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
                warn!("incomplete response, need more {} bytes",
                      if let Needed::Size(size) = needed {
                          size.to_string()
                      } else {
                          "unknown".to_owned()
                      });

                debug!("\n{}", hexdump!(buf));

                Ok(None)
            }
            IResult::Error(err) => {
                if log_enabled!(Debug) {
                    display_parse_error::<KafkaResponse>(&buf[..], IResult::Error(err.clone()));
                }

                Err(io::Error::new(io::ErrorKind::InvalidData,
                                   format!("fail to parse response, {}", err)))
            }
        }
    }
}

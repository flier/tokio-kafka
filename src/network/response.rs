use std::io;

use log::LogLevel::Debug;

use nom::{IResult, Needed};

use protocol::{ApiKeys, ProduceResponse, parse_produce_response, FetchResponse,
               parse_fetch_response, ListOffsetResponse, parse_list_offset_response,
               MetadataResponse, parse_metadata_response, ApiVersionsResponse,
               parse_api_versions_response, display_parse_error};

#[derive(Clone, Debug, PartialEq)]
pub enum KafkaResponse {
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    ListOffsets(ListOffsetResponse),
    Metadata(MetadataResponse),
    ApiVersions(ApiVersionsResponse),
}

impl KafkaResponse {
    pub fn parse(buf: &[u8], api_key: ApiKeys, api_version: i16) -> io::Result<Option<Self>> {
        debug!("parsing {} bytes response as {:?} ({:?})",
               buf.len(),
               api_key,
               api_version);

        let res = match api_key {
            ApiKeys::Produce => {
                parse_produce_response(buf, api_version as i16).map(KafkaResponse::Produce)
            }
            ApiKeys::Fetch => {
                parse_fetch_response(buf, api_version as i16).map(KafkaResponse::Fetch)
            }
            ApiKeys::ListOffsets => {
                parse_list_offset_response(buf, api_version as i16).map(KafkaResponse::ListOffsets)
            }
            ApiKeys::Metadata => parse_metadata_response(buf).map(KafkaResponse::Metadata),
            ApiKeys::ApiVersions => {
                parse_api_versions_response(buf).map(KafkaResponse::ApiVersions)
            }
            _ => {
                warn!("unsupport response type, {:?}", api_key);

                IResult::Incomplete(Needed::Unknown)
            }
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

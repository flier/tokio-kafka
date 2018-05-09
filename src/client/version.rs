#![allow(non_camel_case_types)]
use std::str::FromStr;
use std::iter::FromIterator;

use semver::{Identifier, Version as SemVersion};

use errors::{Error, Result};
use protocol::{ApiKeys, RecordFormat, UsableApiVersion, UsableApiVersions};

/// Kafka version for API requests version
///
/// See [`ClientConfig::broker_version_fallback`](struct.ClientConfig.html#broker_version_fallback.
/// v)

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[repr(u32)]
pub enum KafkaVersion {
    #[serde(rename = "0.8.0")]
    KAFKA_0_8_0,
    #[serde(rename = "0.8.1")]
    KAFKA_0_8_1,
    #[serde(rename = "0.8.2")]
    KAFKA_0_8_2,
    #[serde(rename = "0.9.0")]
    KAFKA_0_9_0,
    /// 0.10.0-IV0 is introduced for KIP-31/32 which changes the message format.
    #[serde(rename = "0.10.0-IV0")]
    KAFKA_0_10_0_IV0,
    /// 0.10.0-IV1 is introduced for KIP-36(rack awareness) and KIP-43(SASL
    /// handshake).
    #[serde(rename = "0.10.0")]
    KAFKA_0_10_0_IV1,
    /// introduced for JoinGroup protocol change in KIP-62
    #[serde(rename = "0.10.1-IV0")]
    KAFKA_0_10_1_IV0,
    /// 0.10.1-IV1 is introduced for KIP-74(fetch response size limit).
    #[serde(rename = "0.10.1-IV1")]
    KAFKA_0_10_1_IV1,
    /// introduced ListOffsetRequest v1 in KIP-79
    #[serde(rename = "0.10.1")]
    KAFKA_0_10_1_IV2,
    /// introduced UpdateMetadataRequest v3 in KIP-103
    #[serde(rename = "0.10.2")]
    KAFKA_0_10_2_IV0,
    /// KIP-98 (idempotent and transactional producer support)
    #[serde(rename = "0.11.0-IV0")]
    KAFKA_0_11_0_IV0,
    /// introduced DeleteRecordsRequest v0 and FetchRequest v4 in KIP-107
    #[serde(rename = "0.11.0-IV1")]
    KAFKA_0_11_0_IV1,
    /// Introduced leader epoch fetches to the replica fetcher via KIP-101
    #[serde(rename = "0.11.0")]
    KAFKA_0_11_0_IV2,
    /// Introduced LeaderAndIsrRequest V1, UpdateMetadataRequest V4 and
    /// FetchRequest V6 via KIP-112
    #[serde(rename = "1.0")]
    KAFKA_1_0_IV0,
    /// Introduced DeleteGroupsRequest V0 via KIP-229, plus KIP-227 incremental fetch requests,
    /// and KafkaStorageException for fetch requests.
    #[serde(rename = "1.1")]
    KAFKA_1_1_IV0,
    #[serde(rename = "2.0")]
    KAFKA_2_0_IV0,
}

impl KafkaVersion {
    pub fn supported_api_versions() -> &'static UsableApiVersions {
        &*SUPPORTED_API_VERSIONS
    }

    pub fn api_versions(&self) -> Option<&UsableApiVersions> {
        match *self {
            KafkaVersion::KAFKA_0_8_0 => Some(&*KAFKA_0_8_0_VERSIONS),
            KafkaVersion::KAFKA_0_8_1 => Some(&*KAFKA_0_8_1_VERSIONS),
            KafkaVersion::KAFKA_0_8_2 => Some(&*KAFKA_0_8_2_VERSIONS),
            KafkaVersion::KAFKA_0_9_0 => Some(&*KAFKA_0_9_0_VERSIONS),
            _ => None,
        }
    }
}

impl From<KafkaVersion> for SemVersion {
    fn from(version: KafkaVersion) -> Self {
        match version {
            KafkaVersion::KAFKA_0_8_0 => SemVersion::new(0, 8, 0),
            KafkaVersion::KAFKA_0_8_1 => SemVersion::new(0, 8, 1),
            KafkaVersion::KAFKA_0_8_2 => SemVersion::new(0, 8, 2),
            KafkaVersion::KAFKA_0_9_0 => SemVersion::new(0, 9, 0),
            KafkaVersion::KAFKA_0_10_0_IV0 => SemVersion {
                major: 0,
                minor: 10,
                patch: 0,
                pre: vec![],
                build: vec![Identifier::AlphaNumeric("IV0".to_owned())],
            },
            KafkaVersion::KAFKA_0_10_0_IV1 => SemVersion {
                major: 0,
                minor: 10,
                patch: 0,
                pre: vec![],
                build: vec![Identifier::AlphaNumeric("IV1".to_owned())],
            },
            KafkaVersion::KAFKA_0_10_1_IV0 => SemVersion {
                major: 0,
                minor: 10,
                patch: 1,
                pre: vec![],
                build: vec![Identifier::AlphaNumeric("IV0".to_owned())],
            },
            KafkaVersion::KAFKA_0_10_1_IV1 => SemVersion {
                major: 0,
                minor: 10,
                patch: 1,
                pre: vec![],
                build: vec![Identifier::AlphaNumeric("IV1".to_owned())],
            },
            KafkaVersion::KAFKA_0_10_1_IV2 => SemVersion::new(0, 10, 1),
            KafkaVersion::KAFKA_0_10_2_IV0 => SemVersion::new(0, 10, 2),
            KafkaVersion::KAFKA_0_11_0_IV0 => SemVersion {
                major: 0,
                minor: 11,
                patch: 0,
                pre: vec![],
                build: vec![Identifier::AlphaNumeric("IV0".to_owned())],
            },
            KafkaVersion::KAFKA_0_11_0_IV1 => SemVersion {
                major: 0,
                minor: 11,
                patch: 0,
                pre: vec![],
                build: vec![Identifier::AlphaNumeric("IV1".to_owned())],
            },
            KafkaVersion::KAFKA_0_11_0_IV2 => SemVersion::new(0, 11, 0),
            KafkaVersion::KAFKA_1_0_IV0 => SemVersion::new(1, 0, 0),
            KafkaVersion::KAFKA_1_1_IV0 => SemVersion::new(1, 1, 0),
            KafkaVersion::KAFKA_2_0_IV0 => SemVersion::new(2, 0, 0),
        }
    }
}

impl RecordFormat {
    pub fn min_kafka_version(self) -> KafkaVersion {
        match self {
            RecordFormat::V0 => KafkaVersion::KAFKA_0_8_0,
            RecordFormat::V1 => KafkaVersion::KAFKA_0_10_0_IV1,
            RecordFormat::V2 => KafkaVersion::KAFKA_0_11_0_IV2,
        }
    }
}

impl FromStr for KafkaVersion {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        parse_api_versions(s.as_bytes()).to_result().map_err(|err| err.into())
    }
}

named!(
    parse_api_versions<KafkaVersion>,
    alt_complete!(
        tag!("0.8.0")       => { |_| KafkaVersion::KAFKA_0_8_0 } |
        tag!("0.8.1")       => { |_| KafkaVersion::KAFKA_0_8_1 } |
        tag!("0.8.2")       => { |_| KafkaVersion::KAFKA_0_8_2 } |
        tag!("0.8")         => { |_| KafkaVersion::KAFKA_0_8_2 } |
        tag!("0.9.0")       => { |_| KafkaVersion::KAFKA_0_9_0 } |
        tag!("0.9")         => { |_| KafkaVersion::KAFKA_0_9_0 } |
        tag!("0.10.0-IV0")  => { |_| KafkaVersion::KAFKA_0_10_0_IV0 } |
        tag!("0.10.0-IV1")  => { |_| KafkaVersion::KAFKA_0_10_0_IV1 } |
        tag!("0.10.0")      => { |_| KafkaVersion::KAFKA_0_10_0_IV1 } |
        tag!("0.10.1-IV0")  => { |_| KafkaVersion::KAFKA_0_10_1_IV0 } |
        tag!("0.10.1-IV1")  => { |_| KafkaVersion::KAFKA_0_10_1_IV1 } |
        tag!("0.10.1-IV2")  => { |_| KafkaVersion::KAFKA_0_10_1_IV2 } |
        tag!("0.10.1")      => { |_| KafkaVersion::KAFKA_0_10_1_IV2 } |
        tag!("0.10.2-IV0")  => { |_| KafkaVersion::KAFKA_0_10_2_IV0 } |
        tag!("0.10.2")      => { |_| KafkaVersion::KAFKA_0_10_2_IV0 } |
        tag!("0.10")        => { |_| KafkaVersion::KAFKA_0_10_2_IV0 } |
        tag!("0.11.0-IV0")  => { |_| KafkaVersion::KAFKA_0_11_0_IV0 } |
        tag!("0.11.0-IV1")  => { |_| KafkaVersion::KAFKA_0_11_0_IV1 } |
        tag!("0.11.0-IV2")  => { |_| KafkaVersion::KAFKA_0_11_0_IV2 } |
        tag!("0.11.0")      => { |_| KafkaVersion::KAFKA_0_11_0_IV2 } |
        tag!("0.11")        => { |_| KafkaVersion::KAFKA_0_11_0_IV2 } |
        tag!("1.0-IV0")     => { |_| KafkaVersion::KAFKA_1_0_IV0 } |
        tag!("1.0")         => { |_| KafkaVersion::KAFKA_1_0_IV0 } |
        tag!("1.1-IV0")     => { |_| KafkaVersion::KAFKA_1_1_IV0 } |
        tag!("1.1")         => { |_| KafkaVersion::KAFKA_1_1_IV0 } |
        tag!("2.0-IV0")     => { |_| KafkaVersion::KAFKA_2_0_IV0 } |
        tag!("2.0")         => { |_| KafkaVersion::KAFKA_2_0_IV0 }
    )
);

impl Default for KafkaVersion {
    fn default() -> Self {
        KafkaVersion::KAFKA_0_9_0
    }
}

// api versions we support
lazy_static! {
    static ref SUPPORTED_API_VERSIONS: UsableApiVersions = UsableApiVersions::from_iter(vec![
        UsableApiVersion {
            api_key: ApiKeys::Produce,
            min_version: 0,
            max_version: 5,
        },
        UsableApiVersion {
            api_key: ApiKeys::Fetch,
            min_version: 0,
            max_version: 7,
        },
        UsableApiVersion {
            api_key: ApiKeys::ListOffsets,
            min_version: 0,
            max_version: 1,
        },
        UsableApiVersion {
            api_key: ApiKeys::OffsetCommit,
            min_version: 0,
            max_version: 2,
        },
        UsableApiVersion {
            api_key: ApiKeys::OffsetFetch,
            min_version: 0,
            max_version: 1,
        },
        UsableApiVersion {
            api_key: ApiKeys::JoinGroup,
            min_version: 0,
            max_version: 1,
        },
    ]);
    static ref KAFKA_0_8_0_VERSIONS: UsableApiVersions = UsableApiVersions::from_iter(vec![
        UsableApiVersion {
            api_key: ApiKeys::Produce,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::Fetch,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::ListOffsets,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::Metadata,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::LeaderAndIsr,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::StopReplica,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::UpdateMetadata,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::ControlledShutdown,
            min_version: 0,
            max_version: 0,
        },
    ]);
    static ref KAFKA_0_8_1_VERSIONS: UsableApiVersions = UsableApiVersions::from_iter(vec![
        UsableApiVersion {
            api_key: ApiKeys::Produce,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::Fetch,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::ListOffsets,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::Metadata,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::LeaderAndIsr,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::StopReplica,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::UpdateMetadata,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::ControlledShutdown,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::OffsetCommit,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::OffsetFetch,
            min_version: 0,
            max_version: 0,
        },
    ]);
    static ref KAFKA_0_8_2_VERSIONS: UsableApiVersions = UsableApiVersions::from_iter(vec![
        UsableApiVersion {
            api_key: ApiKeys::Produce,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::Fetch,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::ListOffsets,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::Metadata,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::LeaderAndIsr,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::StopReplica,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::UpdateMetadata,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::ControlledShutdown,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::OffsetCommit,
            min_version: 0,
            max_version: 1,
        },
        UsableApiVersion {
            api_key: ApiKeys::OffsetFetch,
            min_version: 0,
            max_version: 1,
        },
        UsableApiVersion {
            api_key: ApiKeys::GroupCoordinator,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::JoinGroup,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::Heartbeat,
            min_version: 0,
            max_version: 0,
        },
    ]);
    static ref KAFKA_0_9_0_VERSIONS: UsableApiVersions = UsableApiVersions::from_iter(vec![
        UsableApiVersion {
            api_key: ApiKeys::Produce,
            min_version: 0,
            max_version: 1,
        },
        UsableApiVersion {
            api_key: ApiKeys::Fetch,
            min_version: 0,
            max_version: 1,
        },
        UsableApiVersion {
            api_key: ApiKeys::ListOffsets,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::Metadata,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::LeaderAndIsr,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::StopReplica,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::UpdateMetadata,
            min_version: 0,
            max_version: 1,
        },
        UsableApiVersion {
            api_key: ApiKeys::ControlledShutdown,
            min_version: 0,
            max_version: 1,
        },
        UsableApiVersion {
            api_key: ApiKeys::OffsetCommit,
            min_version: 0,
            max_version: 2,
        },
        UsableApiVersion {
            api_key: ApiKeys::OffsetFetch,
            min_version: 0,
            max_version: 1,
        },
        UsableApiVersion {
            api_key: ApiKeys::GroupCoordinator,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::JoinGroup,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::Heartbeat,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::LeaveGroup,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::SyncGroup,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::DescribeGroups,
            min_version: 0,
            max_version: 0,
        },
        UsableApiVersion {
            api_key: ApiKeys::ListGroups,
            min_version: 0,
            max_version: 0,
        },
    ]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kafka_versions() {
        let api_versions = vec![
            ("0.8.0", KafkaVersion::KAFKA_0_8_0),
            ("0.8.0.0", KafkaVersion::KAFKA_0_8_0),
            ("0.8.2", KafkaVersion::KAFKA_0_8_2),
            ("0.9.0", KafkaVersion::KAFKA_0_9_0),
            ("0.10.0-IV0", KafkaVersion::KAFKA_0_10_0_IV0),
            ("0.10.0-IV1", KafkaVersion::KAFKA_0_10_0_IV1),
            ("0.10.0", KafkaVersion::KAFKA_0_10_0_IV1),
            ("0.10.1-IV0", KafkaVersion::KAFKA_0_10_1_IV0),
            ("0.10.1-IV1", KafkaVersion::KAFKA_0_10_1_IV1),
            ("0.10.1-IV2", KafkaVersion::KAFKA_0_10_1_IV2),
            ("0.10.1", KafkaVersion::KAFKA_0_10_1_IV2),
            ("0.10.2-IV0", KafkaVersion::KAFKA_0_10_2_IV0),
            ("0.10.2", KafkaVersion::KAFKA_0_10_2_IV0),
            ("0.11.0-IV0", KafkaVersion::KAFKA_0_11_0_IV0),
            ("0.11.0-IV1", KafkaVersion::KAFKA_0_11_0_IV1),
            ("0.11.0-IV2", KafkaVersion::KAFKA_0_11_0_IV2),
            ("0.11.0", KafkaVersion::KAFKA_0_11_0_IV2),
            ("1.0-IV0", KafkaVersion::KAFKA_1_0_IV0),
            ("1.0", KafkaVersion::KAFKA_1_0_IV0),
            ("1.1-IV0", KafkaVersion::KAFKA_1_1_IV0),
            ("1.1", KafkaVersion::KAFKA_1_1_IV0),
        ];

        for (s, version) in api_versions {
            assert_eq!(s.parse::<KafkaVersion>().unwrap(), version, "parse API version: {}", s);
        }
    }
}

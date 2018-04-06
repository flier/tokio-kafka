use std::mem;
use std::fmt;
use std::result::Result as StdResult;
use std::str::FromStr;

use serde::ser::{Serialize, Serializer};
use serde::de::{self, Deserialize, Deserializer, Visitor};

use errors::{Error, ErrorKind, Result};
use protocol::{ApiKeys, UsableApiVersion, UsableApiVersions};

/// Kafka version for API requests version
///
/// See [`ClientConfig::broker_version_fallback`](struct.ClientConfig.html#broker_version_fallback.v)
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u16)]
pub enum KafkaVersion {
    /// Kafka 0.8.0
    KAFKA_0_8_0 = 800,
    /// Kafka 0.8.1.0, 0.8.1.1
    KAFKA_0_8_1 = 801,
    /// Kafka 0.8.2.0, 0.8.2.2
    KAFKA_0_8_2 = 802,
    /// Kafka 0.9.0.0, 0.9.0.1
    KAFKA_0_9_0 = 900,
}

impl KafkaVersion {
    pub fn version(&self) -> &'static str {
        match *self {
            KafkaVersion::KAFKA_0_8_0 => "0.8.0",
            KafkaVersion::KAFKA_0_8_1 => "0.8.1",
            KafkaVersion::KAFKA_0_8_2 => "0.8.2",
            KafkaVersion::KAFKA_0_9_0 => "0.9.0",
        }
    }

    pub fn value(&self) -> u16 {
        unsafe { mem::transmute(*self) }
    }

    pub fn api_versions(&self) -> &UsableApiVersions {
        match *self {
            KafkaVersion::KAFKA_0_8_0 => &*KAFKA_0_8_0_VERSIONS,
            KafkaVersion::KAFKA_0_8_1 => &*KAFKA_0_8_1_VERSIONS,
            KafkaVersion::KAFKA_0_8_2 => &*KAFKA_0_8_2_VERSIONS,
            KafkaVersion::KAFKA_0_9_0 => &*KAFKA_0_9_0_VERSIONS,
        }
    }
}

impl From<u16> for KafkaVersion {
    fn from(v: u16) -> Self {
        unsafe { mem::transmute(v) }
    }
}

impl Default for KafkaVersion {
    fn default() -> Self {
        KafkaVersion::KAFKA_0_9_0
    }
}

impl FromStr for KafkaVersion {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "0.8.0" => Ok(KafkaVersion::KAFKA_0_8_0),
            "0.8.1" => Ok(KafkaVersion::KAFKA_0_8_1),
            "0.8.2" => Ok(KafkaVersion::KAFKA_0_8_2),
            "0.9.0" => Ok(KafkaVersion::KAFKA_0_9_0),
            _ => bail!(ErrorKind::ParseError(format!("unknown kafka version: {}", s))),
        }
    }
}

impl fmt::Display for KafkaVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.version())
    }
}

impl Serialize for KafkaVersion {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.serialize_str(self.version())
    }
}

impl<'de> Deserialize<'de> for KafkaVersion {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
        where D: Deserializer<'de>
    {
        struct KafkaVersionVistor;

        impl<'de> Visitor<'de> for KafkaVersionVistor {
            type Value = KafkaVersion;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("Valid values are: 0.9.0, 0.8.2, 0.8.1, 0.8.0.")
            }

            fn visit_str<E>(self, v: &str) -> StdResult<Self::Value, E>
                where E: de::Error
            {
                v.parse().map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(KafkaVersionVistor)
    }
}

lazy_static! {
    static ref KAFKA_0_8_0_VERSIONS: UsableApiVersions = UsableApiVersions::new(vec![
        UsableApiVersion { api_key: ApiKeys::Produce,               min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::Fetch,                 min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::ListOffsets,           min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::Metadata,              min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::LeaderAndIsr,          min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::StopReplica,           min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::UpdateMetadata,        min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::ControlledShutdown,    min_version: 0, max_version: 0, },
    ]);
    static ref KAFKA_0_8_1_VERSIONS: UsableApiVersions = UsableApiVersions::new(vec![
        UsableApiVersion { api_key: ApiKeys::Produce,               min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::Fetch,                 min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::ListOffsets,           min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::Metadata,              min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::LeaderAndIsr,          min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::StopReplica,           min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::UpdateMetadata,        min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::ControlledShutdown,    min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::OffsetCommit,          min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::OffsetFetch,           min_version: 0, max_version: 0, },
    ]);
    static ref KAFKA_0_8_2_VERSIONS: UsableApiVersions = UsableApiVersions::new(vec![
        UsableApiVersion { api_key: ApiKeys::Produce,               min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::Fetch,                 min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::ListOffsets,           min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::Metadata,              min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::LeaderAndIsr,          min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::StopReplica,           min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::UpdateMetadata,        min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::ControlledShutdown,    min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::OffsetCommit,          min_version: 0, max_version: 1, },
        UsableApiVersion { api_key: ApiKeys::OffsetFetch,           min_version: 0, max_version: 1, },
        UsableApiVersion { api_key: ApiKeys::GroupCoordinator,      min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::JoinGroup,             min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::Heartbeat,             min_version: 0, max_version: 0, },
    ]);
    static ref  KAFKA_0_9_0_VERSIONS: UsableApiVersions = UsableApiVersions::new(vec![
        UsableApiVersion { api_key: ApiKeys::Produce,               min_version: 0, max_version: 1, },
        UsableApiVersion { api_key: ApiKeys::Fetch,                 min_version: 0, max_version: 1, },
        UsableApiVersion { api_key: ApiKeys::ListOffsets,           min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::Metadata,              min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::LeaderAndIsr,          min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::StopReplica,           min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::UpdateMetadata,        min_version: 0, max_version: 1, },
        UsableApiVersion { api_key: ApiKeys::ControlledShutdown,    min_version: 0, max_version: 1, },
        UsableApiVersion { api_key: ApiKeys::OffsetCommit,          min_version: 0, max_version: 2, },
        UsableApiVersion { api_key: ApiKeys::OffsetFetch,           min_version: 0, max_version: 1, },
        UsableApiVersion { api_key: ApiKeys::GroupCoordinator,      min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::JoinGroup,             min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::Heartbeat,             min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::LeaveGroup,            min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::SyncGroup,             min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::DescribeGroups,        min_version: 0, max_version: 0, },
        UsableApiVersion { api_key: ApiKeys::ListGroups,            min_version: 0, max_version: 0, },
    ]);
}

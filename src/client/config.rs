use std::time::Duration;
use std::net::SocketAddr;

use tokio_timer::{Timer, wheel};
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use client::KafkaVersion;

/// The default milliseconds after which we close the idle connections.
///
/// Defaults to 5 seconds, see [`ClientConfig::max_connection_idle`](struct.ClientConfig.html#max_connection_idle.v)
pub const DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS: u64 = 5000;

/// The default milliseconds the client will wait for the response of a request.
///
/// Defaults to 30 seconds, see [`ClientConfig::request_timeout`](struct.ClientConfig.html#request_timeout.v)
pub const DEFAULT_REQUEST_TIMEOUT_MILLS: u64 = 30_000;

/// The default milliseconds after which we force a refresh of metadata
///
/// Defaults to 5 minutes, see [`ClientConfig::metadata_max_age`](struct.ClientConfig.html#metadata_max_age.v)
pub const DEFAULT_METADATA_MAX_AGE_MILLS: u64 = 5 * 60 * 1000;

/// The default milliseconds of the timer tick duration.
///
/// Defaults to 100 ms
pub const DEFAULT_TIMER_TICK_MILLS: u64 = 100;

/// The default time to wait before attempting to retry a failed request to a given topic partition.
///
/// Defaults to 100 ms, see [`ClientConfig::retry_backoff`](struct.ClientConfig.html#retry_backoff.v)
pub const DEFAULT_RETRY_BACKOFF_MILLIS: u64 = 100;

/// Configuration for the Kafka Client.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClientConfig {
    /// A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
    #[serde(rename = "bootstrap.servers")]
    pub hosts: Vec<SocketAddr>,

    /// An id string to pass to the server when making requests.
    ///
    /// The purpose of this is to be able to track the source of requests beyond just ip/port
    /// by allowing a logical application name to be included in server-side request logging.
    #[serde(rename = "client.id")]
    pub client_id: Option<String>,

    /// Close idle connections after the number of milliseconds specified by this config.
    #[serde(rename = "connection.max.idle.ms")]
    pub max_connection_idle: u64,

    /// The maximum amount of time the client will wait for the response of a request.
    #[serde(rename = "request.timeout.ms")]
    pub request_timeout: u64,

    /// Request broker's supported API versions to adjust functionality to available protocol features.
    #[serde(rename = "api.version.request")]
    pub api_version_request: bool,

    /// Older broker versions (<0.10.0) provides no way for a client to query for supported protocol features
    /// making it impossible for the client to know what features it may use.
    /// As a workaround a user may set this property to the expected broker version and
    /// the client will automatically adjust its feature set accordingly if the ApiVersionRequest fails (or is disabled).
    /// The fallback broker version will be used for api.version.fallback.ms. Valid values are: 0.9.0, 0.8.2, 0.8.1, 0.8.0.
    #[serde(rename = "broker.version.fallback")]
    pub broker_version_fallback: KafkaVersion,

    /// The period of time in milliseconds after which we force a refresh of metadata
    /// even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions.
    #[serde(rename = "metadata.max.age.ms")]
    pub metadata_max_age: u64,

    /// Record metrics for client operations
    pub metrics: bool,

    /// Setting a value greater than zero will cause the client to resend any record
    /// whose send fails with a potentially transient error.
    pub retries: usize,

    /// The amount of time to wait before attempting to retry a failed request to a given topic partition.
    /// This avoids repeatedly sending requests in a tight loop under some failure scenarios.
    #[serde(rename="retry.backoff.ms")]
    pub retry_backoff: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            hosts: vec![],
            client_id: None,
            max_connection_idle: DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS,
            request_timeout: DEFAULT_REQUEST_TIMEOUT_MILLS,
            api_version_request: false,
            broker_version_fallback: KafkaVersion::default(),
            metadata_max_age: DEFAULT_METADATA_MAX_AGE_MILLS,
            metrics: false,
            retries: 0,
            retry_backoff: DEFAULT_RETRY_BACKOFF_MILLIS,
        }
    }
}

impl ClientConfig {
    /// Construct a `ClientConfig` from brokers
    pub fn from_hosts<I>(hosts: I) -> Self
        where I: Iterator<Item = SocketAddr>
    {
        ClientConfig {
            hosts: hosts.collect(),
            ..Default::default()
        }
    }

    /// Close idle connections after the number of milliseconds specified by this config.
    pub fn max_connection_idle(&self) -> Duration {
        Duration::from_millis(self.max_connection_idle)
    }

    /// The maximum amount of time the client will wait for the response of a request.
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout)
    }

    /// The period of time in milliseconds after which we force a refresh of metadata
    /// even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions.
    pub fn metadata_max_age(&self) -> Duration {
        Duration::from_millis(self.metadata_max_age)
    }

    /// Construct a `Timer`
    pub fn timer(&self) -> Timer {
        wheel()
            .tick_duration(Duration::from_millis(DEFAULT_TIMER_TICK_MILLS))
            .num_slots((self.request_timeout / DEFAULT_TIMER_TICK_MILLS).next_power_of_two() as
                       usize)
            .build()
    }

    /// The amount of time to wait before attempting to retry a failed request to a given topic partition.
    pub fn retry_backoff(&self) -> Duration {
        Duration::from_millis(self.retry_backoff)
    }

    /// The retry strategy when request failed
    pub fn retry_strategy(&self) -> Vec<Duration> {
        ExponentialBackoff::from_millis(self.retry_backoff)
            .map(jitter)
            .take(self.retries)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    extern crate serde_json;

    use super::*;

    #[test]
    fn test_properties() {
        let config = ClientConfig {
            retries: 3,
            ..Default::default()
        };

        assert_eq!(config.max_connection_idle(),
                   Duration::from_millis(DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS));
        assert_eq!(config.request_timeout(),
                   Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MILLS));
        assert_eq!(config.metadata_max_age(),
                   Duration::from_millis(DEFAULT_METADATA_MAX_AGE_MILLS));
        assert_eq!(config.retry_strategy().len(), 3);
    }

    #[test]
    fn test_serialize() {
        let config = ClientConfig {
            hosts: vec!["127.0.0.1:9092".parse().unwrap()],
            client_id: Some("tokio-kafka".to_owned()),
            ..Default::default()
        };
        let json = r#"{
  "bootstrap.servers": [
    "127.0.0.1:9092"
  ],
  "client.id": "tokio-kafka",
  "connection.max.idle.ms": 5000,
  "request.timeout.ms": 30000,
  "api.version.request": false,
  "broker.version.fallback": "0.9.0",
  "metadata.max.age.ms": 300000,
  "metrics": false,
  "retries": 0,
  "retry.backoff.ms": 100
}"#;

        assert_eq!(serde_json::to_string_pretty(&config).unwrap(), json);
        assert_eq!(serde_json::from_str::<ClientConfig>(json).unwrap(), config);
    }
}

use std::ops::{Deref, DerefMut};
use std::time::Duration;

use client::ClientConfig;
use compression::Compression;
use protocol::RequiredAcks;

/// The default amount of time the server will wait for acknowledgments
///
/// Defaults to 30 seconds, see
/// [`ProducerConfig::ack_timeout`](struct.ProducerConfig.html#ack_timeout.v)
pub const DEFAULT_ACK_TIMEOUT_MILLIS: u64 = 30_000;

/// The default bytes that producer will attempt to batch records together into fewer requests
///
/// Defaults to 16 KB, see [`ProducerConfig::batch_size`](struct.ProducerConfig.html#batch_size.v)
pub const DEFAULT_BATCH_SIZE: usize = 16 * 1024;

/// The default maximum size of a request in bytes.
///
/// Defaults to 1 MB, see
/// [`ProducerConfig::max_request_size`](struct.ProducerConfig.html#max_request_size.v)
pub const DEFAULT_MAX_REQUEST_SIZE: usize = 1024 * 1024;

/// The default millionseconds that producer groups together any records
/// that arrive in between request transmissions into a single batched request.
///
/// Defaults to 0 ms, see [`ProducerConfig::linger`](struct.ProducerConfig.html#linger.v)
pub const DEFAULT_LINGER_MILLIS: u64 = 0;

/// Configuration for the `KafkaProducer`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProducerConfig {
    pub client: ClientConfig,

    /// The number of acknowledgments the producer requires the leader
    /// to have received before considering a request complete.
    pub acks: RequiredAcks,

    /// The maximum amount of time the server will wait for acknowledgments
    /// from followers to meet the acknowledgment requirements
    #[serde(rename = "timeout.ms")]
    pub ack_timeout: u64,

    /// The compression type for all data generated by the producer.
    #[serde(rename = "compression.type")]
    pub compression: Compression,

    /// The producer will attempt to batch records together into fewer requests
    /// whenever multiple records are being sent to the same partition.
    #[serde(rename = "batch.size")]
    pub batch_size: usize,

    /// The maximum size of a request in bytes.
    #[serde(rename = "max.request.size")]
    pub max_request_size: usize,

    /// The producer groups together any records
    /// that arrive in between request transmissions into a single batched request.
    #[serde(rename = "linger.ms")]
    pub linger: u64,
}

impl Deref for ProducerConfig {
    type Target = ClientConfig;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for ProducerConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl Default for ProducerConfig {
    fn default() -> Self {
        ProducerConfig {
            client: ClientConfig::default(),
            acks: RequiredAcks::default(),
            ack_timeout: DEFAULT_ACK_TIMEOUT_MILLIS,
            compression: Compression::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            max_request_size: DEFAULT_MAX_REQUEST_SIZE,
            linger: DEFAULT_LINGER_MILLIS,
        }
    }
}

impl ProducerConfig {
    /// Construct a `ProducerConfig` from bootstrap servers of the Kafka cluster
    pub fn with_bootstrap_servers<I>(hosts: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        ProducerConfig {
            client: ClientConfig::with_bootstrap_servers(hosts),
            ..Default::default()
        }
    }

    /// The producer groups together any records
    /// that arrive in between request transmissions into a single batched request.
    pub fn linger(&self) -> Duration {
        Duration::from_millis(self.linger)
    }

    /// The maximum amount of time the server will wait for acknowledgments
    /// from followers to meet the acknowledgment requirements
    pub fn ack_timeout(&self) -> Duration {
        Duration::from_millis(self.ack_timeout)
    }
}

#[cfg(test)]
mod tests {
    extern crate serde_json;

    use super::*;

    #[test]
    fn test_properties() {
        let config = ProducerConfig::default();

        assert_eq!(config.linger(), Duration::from_millis(DEFAULT_LINGER_MILLIS));
        assert_eq!(config.ack_timeout(), Duration::from_millis(DEFAULT_ACK_TIMEOUT_MILLIS));
    }

    #[test]
    fn test_serialize() {
        let config = ProducerConfig::default();
        let json = r#"{
  "client": {
    "bootstrap.servers": [],
    "client.id": null,
    "connection.max.idle.ms": 5000,
    "request.timeout.ms": 30000,
    "api.version.request": false,
    "broker.version.fallback": "0.9.0",
    "metadata.max.age.ms": 300000,
    "metrics": false,
    "retries": 0,
    "retry.backoff.ms": 100
  },
  "acks": "one",
  "timeout.ms": 30000,
  "compression.type": "none",
  "batch.size": 16384,
  "max.request.size": 1048576,
  "linger.ms": 0
}"#;

        assert_eq!(serde_json::to_string_pretty(&config).unwrap(), json);
        assert_eq!(serde_json::from_str::<ProducerConfig>(json).unwrap(), config);
    }
}

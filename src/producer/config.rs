use std::time::Duration;
use std::ops::{Deref, DerefMut};
use std::net::SocketAddr;

use tokio_retry::strategy::{ExponentialBackoff, jitter};

use compression::Compression;
use protocol::RequiredAcks;
use client::ClientConfig;

pub const DEFAULT_ACK_TIMEOUT_MILLIS: u64 = 30_000;
pub const DEFAULT_BATCH_SIZE: usize = 16 * 1024;
pub const DEFAULT_MAX_REQUEST_SIZE: usize = 1024 * 1024;
pub const DEFAULT_LINGER_MILLIS: u64 = 0;
pub const DEFAULT_RETRY_BACKOFF_MILLIS: u64 = 100;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProducerConfig {
    pub client: ClientConfig,

    /// The number of acknowledgments the producer requires the leader
    /// to have received before considering a request complete.
    pub acks: RequiredAcks,

    /// The compression type for all data generated by the producer.
    #[serde(rename = "compression.type")]
    pub compression: Compression,

    /// The producer will attempt to batch records together into fewer requests
    /// whenever multiple records are being sent to the same partition.
    #[serde(rename ="batch.size")]
    pub batch_size: usize,

    /// The maximum size of a request in bytes.
    #[serde(rename ="max.request.size")]
    pub max_request_size: usize,

    /// The producer groups together any records
    /// that arrive in between request transmissions into a single batched request.
    #[serde(rename="linger.ms")]
    pub linger: u64,

    /// Setting a value greater than zero will cause the client to resend any record
    /// whose send fails with a potentially transient error.
    pub retries: usize,

    /// The amount of time to wait before attempting to retry a failed request to a given topic partition.
    /// This avoids repeatedly sending requests in a tight loop under some failure scenarios.
    #[serde(rename="retry.backoff.ms")]
    pub retry_backoff: u64,

    /// The maximum amount of time the server will wait for acknowledgments
    /// from followers to meet the acknowledgment requirements
    #[serde(rename ="timeout.ms")]
    pub ack_timeout: u64,
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
            retries: 0,
            acks: RequiredAcks::default(),
            compression: Compression::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            max_request_size: DEFAULT_MAX_REQUEST_SIZE,
            linger: DEFAULT_LINGER_MILLIS,
            retry_backoff: DEFAULT_RETRY_BACKOFF_MILLIS,
            ack_timeout: DEFAULT_ACK_TIMEOUT_MILLIS,
        }
    }
}

impl ProducerConfig {
    pub fn from_hosts<I>(hosts: I) -> Self
        where I: Iterator<Item = SocketAddr>
    {
        ProducerConfig {
            client: ClientConfig::from_hosts(hosts),
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
    "metrics": false
  },
  "acks": "one",
  "compression.type": "none",
  "batch.size": 16384,
  "max.request.size": 1048576,
  "linger.ms": 0,
  "retries": 0,
  "retry.backoff.ms": 100,
  "timeout.ms": 30000
}"#;

        assert_eq!(serde_json::to_string_pretty(&config).unwrap(), json);
        assert_eq!(serde_json::from_str::<ProducerConfig>(json).unwrap(),
                   config);
    }
}

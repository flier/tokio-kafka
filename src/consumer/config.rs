use std::time::Duration;
use std::ops::{Deref, DerefMut};

use client::ClientConfig;

/// The default milliseconds that the consumer offsets are auto-committed to Kafka.
///
/// Defaults to 5 seconds, see
/// [`ConsumerConfig::auto_commit_interval`](struct.ConsumerConfig.html#auto_commit_interval.v)
pub const DEFAULT_AUTO_COMMIT_INTERVAL_MILLIS: u64 = 5000;

/// The default time between heartbeats to the consumer coordinator when using Kafka's group management facilities.
///
/// Defaults to 3 seconds, see [`ConsumerConfig::heartbeat_interval`](struct.ConsumerConfig.html#heartbeat_interval.v)
pub const DEFAULT_HEARTBEAT_INTERVAL_MILLIS: u64 = 3000;

/// The default maximum delay between invocations of poll() when using consumer group management.
///
/// Defaults to 5 minutes, see [`ConsumerConfig::max_poll_interval`](struct.ConsumerConfig.html#max_poll_interval.v)
pub const DEFAULT_MAX_POLL_INTERVAL_MILLIS: u64 = 300_000;

/// The default maximum number of records returned in a single call to poll().
///
/// Defaults to 500, see [`ConsumerConfig::max_poll_records`](struct.ConsumerConfig.html#max_poll_records.v)
pub const DEFAULT_MAX_POLL_RECORDS: usize = 500;

/// Configuration for the `KafkaConsumer`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConsumerConfig {
    pub client: ClientConfig,

    /// A unique string that identifies the consumer group this consumer belongs to.
    ///
    /// This property is required if the consumer uses either the group management functionality
    /// by using `Consumer::subscribe(topic)` or the Kafka-based offset management strategy.
    #[serde(rename = "group.id")]
    pub group_id: String,

    /// If true the consumer's offset will be periodically committed in the background.
    #[serde(rename = "enable.auto.commit")]
    pub enable_auto_commit: bool,

    /// The frequency in milliseconds that the consumer offsets are auto-committed to Kafka.
    #[serde(rename = "auto.commit.interval.ms")]
    pub auto_commit_interval: u64,

    /// The expected time between heartbeats to the consumer coordinator when using Kafka's group management facilities.
    #[serde(rename = "heartbeat.interval.ms")]
    pub heartbeat_interval: u64,

    /// The maximum delay between invocations of poll() when using consumer group management.
    #[serde(rename = "max.poll.interval.ms")]
    pub max_poll_interval: u64,

    /// The maximum number of records returned in a single call to poll().
    #[serde(rename = "max.poll.records")]
    pub max_poll_records: usize,
}

impl Deref for ConsumerConfig {
    type Target = ClientConfig;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for ConsumerConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        ConsumerConfig {
            client: ClientConfig::default(),
            group_id: "".to_owned(),
            enable_auto_commit: true,
            auto_commit_interval: DEFAULT_AUTO_COMMIT_INTERVAL_MILLIS,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL_MILLIS,
            max_poll_interval: DEFAULT_MAX_POLL_INTERVAL_MILLIS,
            max_poll_records: DEFAULT_MAX_POLL_RECORDS,
        }
    }
}

impl ConsumerConfig {
    /// The frequency in milliseconds that the consumer offsets are auto-committed to Kafka.
    pub fn auto_commit_interval(&self) -> Duration {
        Duration::from_millis(self.auto_commit_interval)
    }

    /// The expected time between heartbeats to the consumer coordinator when using Kafka's group management facilities.
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval)
    }

    /// The maximum delay between invocations of poll() when using consumer group management.
    pub fn max_poll_interval(&self) -> Duration {
        Duration::from_millis(self.max_poll_interval)
    }
}

#[cfg(test)]
mod tests {
    extern crate serde_json;

    use super::*;

    #[test]
    fn test_serialize() {
        let config = ConsumerConfig::default();
        let json = r#"{
  "client": {
    "bootstrap.servers": [],
    "client.id": null,
    "connection.max.idle.ms": 5000,
    "request.timeout.ms": 30000,
    "api.version.request": false,
    "broker.version.fallback": "0.9.0",
    "metadata.max.age.ms": 300000,
    "metrics": false
  },
  "group.id": "",
  "enable.auto.commit": true,
  "auto.commit.interval.ms": 5000,
  "heartbeat.interval.ms": 3000,
  "max.poll.interval.ms": 300000,
  "max.poll.records": 500
}"#;

        assert_eq!(serde_json::to_string_pretty(&config).unwrap(), json);
        assert_eq!(serde_json::from_str::<ConsumerConfig>(json).unwrap(),
                   config);
    }
}

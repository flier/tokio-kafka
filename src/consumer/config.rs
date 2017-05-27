use std::time::Duration;
use std::ops::{Deref, DerefMut};

use client::ClientConfig;
use consumer::AssignmentStrategy;

/// The default milliseconds that the consumer offsets are auto-committed to Kafka.
///
/// Defaults to 5 seconds, see
/// [`ConsumerConfig::auto_commit_interval`](struct.ConsumerConfig.html#auto_commit_interval.v)
pub const DEFAULT_AUTO_COMMIT_INTERVAL_MILLIS: u64 = 5000;

/// The default time between heartbeats to the consumer coordinator when using Kafka's group management facilities.
///
/// Defaults to 3 seconds, see [`ConsumerConfig::heartbeat_interval`](struct.ConsumerConfig.html#heartbeat_interval.v)
pub const DEFAULT_HEARTBEAT_INTERVAL_MILLIS: u64 = 3000;

/// The default maximum number of records returned in a single call to poll().
///
/// Defaults to 500, see [`ConsumerConfig::max_poll_records`](struct.ConsumerConfig.html#max_poll_records.v)
pub const DEFAULT_MAX_POLL_RECORDS: usize = 500;

/// The default timeout used to detect consumer failures when using Kafka's group management facility.
///
/// Defaults to 10 seconds, see [`ConsumerConfig::session_timeout`](struct.ConsumerConfig.html#session_timeout.v)
pub const DEFAULT_SESSION_TIMEOUT_MILLIS: u64 = 10_000;

/// The default maximum delay between invocations of poll() when using consumer group management.
///
/// Defaults to 5 mintues, see [`ConsumerConfig::rebalance_timeout`](struct.ConsumerConfig.html#rebalance_timeout.v)
pub const DEFAULT_REBALANCE_TIMEOUT_MILLIS: u64 = 300_000;

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

    /// The maximum number of records returned in a single call to poll().
    #[serde(rename = "max.poll.records")]
    pub max_poll_records: usize,

    /// Name of partition assignment strategy to use when elected group leader assigns partitions to group members.
    #[serde(rename = "partition.assignment.strategy")]
    pub assignment_strategy: Vec<AssignmentStrategy>,

    /// The timeout used to detect consumer failures when using Kafka's group management facility.
    ///
    /// The consumer sends periodic heartbeats to indicate its liveness to the broker.
    /// If no heartbeats are received by the broker before the expiration of this session timeout,
    /// then the broker will remove this consumer from the group and initiate a rebalance.
    /// Note that the value must be in the allowable range as configured in the broker configuration
    /// by `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
    #[serde(rename = "session.timeout.ms")]
    pub session_timeout: u64,

    /// The maximum delay between invocations of poll() when using consumer group management.
    ///
    /// This places an upper bound on the amount of time that the consumer can be idle before fetching more records.
    /// If poll() is not called before expiration of this timeout, then the consumer is considered failed
    /// and the group will rebalance in order to reassign the partitions to another member.
    #[serde(rename = "max.poll.interval.ms")]
    pub rebalance_timeout: u64,
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
            max_poll_records: DEFAULT_MAX_POLL_RECORDS,
            assignment_strategy: vec![AssignmentStrategy::Range, AssignmentStrategy::RoundRobin],
            session_timeout: DEFAULT_SESSION_TIMEOUT_MILLIS,
            rebalance_timeout: DEFAULT_REBALANCE_TIMEOUT_MILLIS,
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

    /// The timeout used to detect consumer failures when using Kafka's group management facility.
    pub fn session_timeout(&self) -> Duration {
        Duration::from_millis(self.session_timeout)
    }

    /// The maximum delay between invocations of poll() when using consumer group management.
    pub fn rebalance_timeout(&self) -> Duration {
        Duration::from_millis(self.rebalance_timeout)
    }
}

#[cfg(test)]
mod tests {
    extern crate serde_json;

    use super::*;

    #[test]
    fn test_properties() {
        let config = ConsumerConfig::default();

        assert_eq!(config.auto_commit_interval(),
                   Duration::from_millis(DEFAULT_AUTO_COMMIT_INTERVAL_MILLIS));
        assert_eq!(config.heartbeat_interval(),
                   Duration::from_millis(DEFAULT_HEARTBEAT_INTERVAL_MILLIS));
        assert_eq!(config.session_timeout(),
                   Duration::from_millis(DEFAULT_SESSION_TIMEOUT_MILLIS));
        assert_eq!(config.rebalance_timeout(),
                   Duration::from_millis(DEFAULT_REBALANCE_TIMEOUT_MILLIS));
    }

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
  "max.poll.records": 500,
  "partition.assignment.strategy": [
    "range",
    "roundrobin"
  ],
  "session.timeout.ms": 10000,
  "max.poll.interval.ms": 300000
}"#;

        assert_eq!(serde_json::to_string_pretty(&config).unwrap(), json);
        assert_eq!(serde_json::from_str::<ConsumerConfig>(json).unwrap(),
                   config);
    }
}

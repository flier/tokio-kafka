use std::ops::{Deref, DerefMut};
use std::time::Duration;

use client::ClientConfig;
use consumer::{AssignmentStrategy, OffsetResetStrategy};

/// The default milliseconds that the consumer offsets are auto-committed to Kafka.
///
/// Defaults to 5 seconds, see
/// [`ConsumerConfig::auto_commit_interval`](struct.ConsumerConfig.html#auto_commit_interval.v)
pub const DEFAULT_AUTO_COMMIT_INTERVAL_MILLIS: u64 = 5000;

/// The default time between heartbeats to the consumer coordinator when using Kafka's group
/// management facilities.
///
/// Defaults to 3 seconds, see
/// [`ConsumerConfig::heartbeat_interval`](struct.ConsumerConfig.html#heartbeat_interval.v)
pub const DEFAULT_HEARTBEAT_INTERVAL_MILLIS: u64 = 3000;

/// The default maximum number of records returned in a single call to poll().
///
/// Defaults to 500, see
/// [`ConsumerConfig::max_poll_records`](struct.ConsumerConfig.html#max_poll_records.v)
pub const DEFAULT_MAX_POLL_RECORDS: usize = 500;

/// The default timeout used to detect consumer failures when using Kafka's group management
/// facility.
///
/// Defaults to 10 seconds, see
/// [`ConsumerConfig::session_timeout`](struct.ConsumerConfig.html#session_timeout.v)
pub const DEFAULT_SESSION_TIMEOUT_MILLIS: u64 = 10_000;

/// The default maximum delay between invocations of poll() when using consumer group management.
///
/// Defaults to 5 mintues, see
/// [`ConsumerConfig::rebalance_timeout`](struct.ConsumerConfig.html#rebalance_timeout.v)
pub const DEFAULT_REBALANCE_TIMEOUT_MILLIS: u64 = 300_000;

/// The minimum amount of data the server should return for a fetch request.
///
/// Defaults to 1 byte, see
/// [`ConsumerConfig::fetch_min_bytes`](struct.ConsumerConfig.html#fetch_min_bytes.v)
pub const DEFAULT_FETCH_MIN_BYTES: usize = 1;

/// The maximum amount of data the server should return for a fetch request.
///
/// Defaults to 50 `MBytes`, see
/// [`ConsumerConfig::fetch_max_bytes`](struct.ConsumerConfig.html#fetch_max_bytes.v)
pub const DEFAULT_FETCH_MAX_BYTES: usize = 50 * 1024 * 1024;

/// The maximum amount of time the server will block before answering the fetch request
///
/// Defaults to 500 ms, see
/// [`ConsumerConfig::fetch_max_wait`](struct.ConsumerConfig.html#fetch_max_wait.v)
pub const DEFAULT_FETCH_MAX_WAIT_MILLIS: u64 = 500;

/// How long to postpone the next fetch request for a topic+partition in case of a fetch error.
///
/// [`ConsumerConfig::fetch_error_backoff`](struct.ConsumerConfig.html#fetch_error_backoff.v)
pub const DEFAULT_FETCH_ERROR_BACKOFF_MILLIS: u64 = 500;

/// The maximum amount of data per-partition the server will return.
///
/// Defaults to 1 `MBytes`, see
/// [`ConsumerConfig::partition_fetch_bytes`](struct.ConsumerConfig.html#partition_fetch_bytes.v)
pub const DEFAULT_PARTITION_FETCH_BYTES: usize = 1024 * 1024;

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
    pub group_id: Option<String>,

    /// If true the consumer's offset will be periodically committed in the
    /// background.
    #[serde(rename = "enable.auto.commit")]
    pub auto_commit_enabled: bool,

    /// The frequency in milliseconds that the consumer offsets are
    /// auto-committed to Kafka.
    #[serde(rename = "auto.commit.interval.ms")]
    pub auto_commit_interval: u64,

    /// The expected time between heartbeats to the consumer coordinator
    /// when using Kafka's group management facilities.
    #[serde(rename = "heartbeat.interval.ms")]
    pub heartbeat_interval: u64,

    /// The maximum number of records returned in a single call to poll().
    #[serde(rename = "max.poll.records")]
    pub max_poll_records: usize,

    /// Name of partition assignment strategy to use when elected group leader assigns
    /// partitions to group members.
    #[serde(rename = "partition.assignment.strategy")]
    pub assignment_strategy: Vec<AssignmentStrategy>,

    /// The timeout used to detect consumer failures when using Kafka's group management
    /// facility.
    ///
    /// The consumer sends periodic heartbeats to indicate its liveness to the broker.
    /// If no heartbeats are received by the broker before the expiration of this session
    /// timeout, then the broker will remove this consumer from the group and initiate a
    /// rebalance.
    /// Note that the value must be in the allowable range as configured in the broker
    /// configuration by `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
    #[serde(rename = "session.timeout.ms")]
    pub session_timeout: u64,

    /// The maximum delay between invocations of poll() when using consumer group management.
    ///
    /// This places an upper bound on the amount of time that the consumer can be idle before
    /// fetching more records. If poll() is not called before expiration of this timeout, then
    /// the consumer is considered failed and the group will rebalance in order to reassign the
    /// partitions to another member.
    #[serde(rename = "max.poll.interval.ms")]
    pub rebalance_timeout: u64,

    /// What to do when there is no initial offset in Kafka or
    /// if the current offset does not exist any more on the server (e.g. because that data has
    /// been deleted)
    ///
    /// - earliest: automatically reset the offset to the earliest offset
    /// - latest: automatically reset the offset to the latest offset
    /// - none: throw exception to the consumer if no previous offset is found for the
    /// consumer's group
    #[serde(rename = "auto.offset.reset")]
    pub auto_offset_reset: OffsetResetStrategy,

    /// The minimum amount of data the server should return for a fetch request.
    ///
    /// If insufficient data is available the request will wait for that much data to accumulate
    /// before answering the request.
    /// The default setting of 1 byte means that fetch requests are answered as soon as a
    /// single byte of data is available or the fetch request times out waiting for data to
    /// arrive.
    /// Setting this to something greater than 1 will cause the server to wait for larger
    /// amounts of data to accumulate which can improve server throughput a bit at the cost of
    /// some additional latency.
    #[serde(rename = "fetch.min.bytes")]
    pub fetch_min_bytes: usize,

    /// The maximum amount of data the server should return for a fetch request.
    ///
    /// This is not an absolute maximum, if the first message in the first non-empty partition
    /// of the fetch is larger than this value, the message will still be returned to ensure
    /// that the consumer can make progress.
    /// The maximum message size accepted by the broker is defined via `message.max.bytes`
    /// (broker config) or `max.message.bytes` (topic config).
    /// Note that the consumer performs multiple fetches in parallel.
    #[serde(rename = "fetch.max.bytes")]
    pub fetch_max_bytes: usize,

    /// The maximum amount of time the server will block before answering the fetch request
    /// if there isn't sufficient data to immediately satisfy the requirement given by
    /// `fetch.min.bytes`.
    #[serde(rename = "fetch.max.wait.ms")]
    pub fetch_max_wait: u64,

    /// How long to postpone the next fetch request for a topic+partition in
    /// case of a fetch error.
    #[serde(rename = "fetch.error.backoff.ms")]
    pub fetch_error_backoff: u64,

    /// The maximum amount of data per-partition the server will return.
    ///
    /// If the first message in the first non-empty partition of the fetch is larger than this
    /// limit, the message will still be returned to ensure that the consumer can make progress.
    /// The maximum message size accepted by the broker is defined via `message.max.bytes`
    /// (broker config) or `max.message.bytes` (topic config).
    #[serde(rename = "max.partition.fetch.bytes")]
    pub partition_fetch_bytes: usize,

    /// The maximum amount of data the consumer should prefetch for records.
    #[serde(rename = "prefetch.low.watermark.bytes")]
    pub prefetch_low_watermark: usize,

    /// The maximum amount of data the consumer should prefetch for records.
    #[serde(rename = "prefetch.high.watermark.bytes")]
    pub prefetch_high_watermark: usize,
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
            group_id: None,
            auto_commit_enabled: true,
            auto_commit_interval: DEFAULT_AUTO_COMMIT_INTERVAL_MILLIS,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL_MILLIS,
            max_poll_records: DEFAULT_MAX_POLL_RECORDS,
            assignment_strategy: vec![AssignmentStrategy::Range, AssignmentStrategy::RoundRobin],
            session_timeout: DEFAULT_SESSION_TIMEOUT_MILLIS,
            rebalance_timeout: DEFAULT_REBALANCE_TIMEOUT_MILLIS,
            auto_offset_reset: OffsetResetStrategy::default(),
            fetch_min_bytes: DEFAULT_FETCH_MIN_BYTES,
            fetch_max_bytes: DEFAULT_FETCH_MAX_BYTES,
            fetch_max_wait: DEFAULT_FETCH_MAX_WAIT_MILLIS,
            fetch_error_backoff: DEFAULT_FETCH_ERROR_BACKOFF_MILLIS,
            partition_fetch_bytes: DEFAULT_PARTITION_FETCH_BYTES,
            prefetch_low_watermark: 0,
            prefetch_high_watermark: 0,
        }
    }
}

impl ConsumerConfig {
    /// Construct a `ConsumerConfig` from bootstrap servers of the Kafka cluster
    pub fn with_bootstrap_servers<I>(hosts: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        ConsumerConfig {
            client: ClientConfig::with_bootstrap_servers(hosts),
            ..Default::default()
        }
    }

    /// The frequency in milliseconds that the consumer offsets are
    /// auto-committed to Kafka.
    pub fn auto_commit_interval(&self) -> Option<Duration> {
        if self.auto_commit_enabled {
            Some(Duration::from_millis(self.auto_commit_interval))
        } else {
            None
        }
    }

    /// The expected time between heartbeats to the consumer coordinator when using Kafka's
    /// group management facilities.
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval)
    }

    /// The timeout used to detect consumer failures when using Kafka's group management
    /// facility.
    pub fn session_timeout(&self) -> Duration {
        Duration::from_millis(self.session_timeout)
    }

    /// The maximum delay between invocations of poll() when using consumer
    /// group management.
    pub fn rebalance_timeout(&self) -> Duration {
        Duration::from_millis(self.rebalance_timeout)
    }

    /// The maximum amount of time the server will block before answering the fetch request
    /// if there isn't sufficient data to immediately satisfy the requirement given by
    /// `fetch.min.bytes`.
    pub fn fetch_max_wait(&self) -> Duration {
        Duration::from_millis(self.fetch_max_wait)
    }

    /// How long to postpone the next fetch request for a topic+partition in
    /// case of a fetch error.
    pub fn fetch_error_backoff(&self) -> Duration {
        Duration::from_millis(self.fetch_error_backoff)
    }

    pub fn prefetch_enabled(&self) -> bool {
        self.prefetch_low_watermark > 0 && self.prefetch_high_watermark > self.prefetch_low_watermark
    }
}

#[cfg(test)]
mod tests {
    extern crate serde_json;

    use super::*;

    #[test]
    fn test_properties() {
        let config = ConsumerConfig::default();

        assert_eq!(
            config.auto_commit_interval(),
            Some(Duration::from_millis(DEFAULT_AUTO_COMMIT_INTERVAL_MILLIS))
        );
        assert_eq!(
            config.heartbeat_interval(),
            Duration::from_millis(DEFAULT_HEARTBEAT_INTERVAL_MILLIS)
        );
        assert_eq!(
            config.session_timeout(),
            Duration::from_millis(DEFAULT_SESSION_TIMEOUT_MILLIS)
        );
        assert_eq!(
            config.rebalance_timeout(),
            Duration::from_millis(DEFAULT_REBALANCE_TIMEOUT_MILLIS)
        );
        assert_eq!(
            config.fetch_max_wait(),
            Duration::from_millis(DEFAULT_FETCH_MAX_WAIT_MILLIS)
        );
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
    "metrics": false,
    "retries": 0,
    "retry.backoff.ms": 100
  },
  "group.id": null,
  "enable.auto.commit": true,
  "auto.commit.interval.ms": 5000,
  "heartbeat.interval.ms": 3000,
  "max.poll.records": 500,
  "partition.assignment.strategy": [
    "range",
    "roundrobin"
  ],
  "session.timeout.ms": 10000,
  "max.poll.interval.ms": 300000,
  "auto.offset.reset": "latest",
  "fetch.min.bytes": 1,
  "fetch.max.bytes": 52428800,
  "fetch.max.wait.ms": 500,
  "fetch.error.backoff.ms": 500,
  "max.partition.fetch.bytes": 1048576,
  "prefetch.low.watermark.bytes": 0,
  "prefetch.high.watermark.bytes": 0
}"#;

        assert_eq!(serde_json::to_string_pretty(&config).unwrap(), json);
        assert_eq!(serde_json::from_str::<ConsumerConfig>(json).unwrap(), config);
    }
}

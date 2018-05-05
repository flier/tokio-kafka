use std::ops::{Deref, DerefMut};
use std::time::Duration;

use tokio_core::reactor::Handle;

use client::{KafkaClient, KafkaVersion};
use consumer::{AssignmentStrategy, ConsumerConfig, KafkaConsumer, OffsetResetStrategy};
use errors::{ErrorKind, Result};
use protocol::ToMilliseconds;
use serialization::{Deserializer, NoopDeserializer};

/// A `KafkaConsumer` builder easing the process of setting up various
/// configuration settings.
pub struct ConsumerBuilder<'a, K, V> {
    config: ConsumerConfig,
    client: Option<KafkaClient<'a>>,
    handle: Option<Handle>,
    key_deserializer: Option<K>,
    value_deserializer: Option<V>,
}

impl<'a, K, V> Deref for ConsumerBuilder<'a, K, V> {
    type Target = ConsumerConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl<'a, K, V> DerefMut for ConsumerBuilder<'a, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl<'a, K, V> Default for ConsumerBuilder<'a, K, V> {
    fn default() -> Self {
        ConsumerBuilder {
            config: ConsumerConfig::default(),
            client: None,
            handle: None,
            key_deserializer: None,
            value_deserializer: None,
        }
    }
}

impl<'a, K, V> From<KafkaClient<'a>> for ConsumerBuilder<'a, K, V> {
    fn from(client: KafkaClient<'a>) -> Self {
        ConsumerBuilder {
            client: Some(client),
            ..Default::default()
        }
    }
}

impl<'a, K, V> ConsumerBuilder<'a, K, V> {
    /// Construct a `ConsumerBuilder` from ConsumerConfig
    pub fn with_config(config: ConsumerConfig, handle: Handle) -> Self {
        ConsumerBuilder {
            config,
            handle: Some(handle),
            ..Default::default()
        }
    }

    /// Construct a `ConsumerBuilder` from bootstrap servers of the Kafka
    /// cluster
    pub fn with_bootstrap_servers<I>(hosts: I, handle: Handle) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        Self::with_config(ConsumerConfig::with_bootstrap_servers(hosts), handle)
    }

    fn with_client(mut self, client: KafkaClient<'a>) -> Self {
        self.client = Some(client);
        self
    }

    fn with_handle(mut self, handle: Handle) -> Self {
        self.handle = Some(handle);
        self
    }

    /// Sets the id string to pass to the server when making requests.
    pub fn with_client_id<S>(mut self, client_id: S) -> Self
    where
        S: Into<String>,
    {
        self.config.client_id = Some(client_id.into());
        self
    }

    /// Sets the number of milliseconds after this we close idle connections
    pub fn with_max_connection_idle(mut self, max_connection_idle: Duration) -> Self {
        self.config.max_connection_idle = max_connection_idle.as_millis();
        self
    }

    /// Sets the maximum amount of time the client will wait for the response
    /// of a request.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.config.request_timeout = request_timeout.as_millis();
        self
    }

    /// Sets the request broker's supported API versions to adjust functionality to available
    /// protocol features.
    pub fn with_api_version_request(mut self) -> Self {
        self.config.api_version_request = true;
        self
    }

    /// Sets the fallback broker version will be used
    pub fn with_broker_version_fallback(mut self, version: KafkaVersion) -> Self {
        self.config.broker_version_fallback = version;
        self
    }

    /// Sets the period of time in milliseconds after which we force a refresh
    /// of metadata
    pub fn with_metadata_max_age(mut self, metadata_max_age: Duration) -> Self {
        self.config.metadata_max_age = metadata_max_age.as_millis();
        self
    }

    /// Sets to record metrics for client operations
    pub fn with_metrics(mut self) -> Self {
        self.config.metrics = true;
        self
    }

    /// Sets the unique string that identifies the consumer group this consumer
    /// belongs to.
    pub fn with_group_id<S>(mut self, group_id: S) -> Self
    where
        S: Into<String>,
    {
        self.config.group_id = Some(group_id.into());
        self
    }

    /// What to do when there is no initial offset in Kafka or
    /// if the current offset does not exist any more on the server
    pub fn with_auto_offset_reset(mut self, strategy: OffsetResetStrategy) -> Self {
        self.config.auto_offset_reset = strategy;
        self
    }

    /// Sets to disable the consumer's offset will be periodically committed in
    /// the background.
    pub fn without_auto_commit(mut self) -> Self {
        self.config.auto_commit_enabled = false;
        self
    }

    /// Sets the frequency in milliseconds that the consumer offsets are auto-committed to
    /// Kafka.
    pub fn with_auto_commit_interval(mut self, auto_commit_interval: Duration) -> Self {
        self.config.auto_commit_interval = auto_commit_interval.as_millis();
        self
    }

    /// Sets the expected time between heartbeats to the consumer coordinator when using
    /// Kafka's group management facilities.
    pub fn with_heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        self.config.heartbeat_interval = heartbeat_interval.as_millis();
        self
    }

    /// Sets the maximum number of records returned in a single call to poll().
    pub fn with_max_poll_records(mut self, max_poll_records: usize) -> Self {
        self.config.max_poll_records = max_poll_records.to_owned();
        self
    }

    /// Sets the partition assignment strategy to use when elected group leader assigns
    /// partitions to group members.
    pub fn with_assignment_strategy(mut self, assignment_strategy: Vec<AssignmentStrategy>) -> Self {
        self.config.assignment_strategy = assignment_strategy;
        self
    }

    /// Sets timeout used to detect consumer failures when using Kafka's group management
    /// facility.
    pub fn with_session_timeout(mut self, session_timeout: Duration) -> Self {
        self.config.session_timeout = session_timeout.as_millis();
        self
    }

    /// Sets the maximum delay between invocations of poll() when using consumer group
    /// management.
    pub fn with_rebalance_timeout(mut self, rebalance_timeout: Duration) -> Self {
        self.config.rebalance_timeout = rebalance_timeout.as_millis();
        self
    }

    /// Sets the maximum amount of data the server should return for a fetch request.
    pub fn with_fetch_max_bytes(mut self, bytes: usize) -> Self {
        self.config.fetch_max_bytes = bytes;
        self
    }

    /// Sets how long to postpone the next fetch request for a topic+partition in
    /// case of a fetch error.
    pub fn with_error_backoff(mut self, backoff: Duration) -> Self {
        self.config.fetch_error_backoff = backoff.as_millis();
        self
    }

    /// The minimum amount of data the server should return for a fetch request.
    pub fn with_fetch_min_bytes(mut self, bytes: usize) -> Self {
        self.config.fetch_min_bytes = bytes;
        self
    }

    /// Sets the maximum amount of data per-partition the server will return.
    pub fn with_partition_max_bytes(mut self, bytes: usize) -> Self {
        self.config.partition_fetch_bytes = bytes;
        self
    }

    /// Sets the key serializer that serialize key to record
    pub fn with_key_deserializer(mut self, key_deserializer: K) -> Self {
        self.key_deserializer = Some(key_deserializer);
        self
    }

    /// Sets the value serializer that serialize value to record
    pub fn with_value_deserializer(mut self, value_deserializer: V) -> Self {
        self.value_deserializer = Some(value_deserializer);
        self
    }
}

impl<'a, V> ConsumerBuilder<'a, NoopDeserializer<()>, V>
where
    V: Deserializer,
{
    /// Sets the key serializer to empty
    pub fn without_key_deserializer(mut self) -> Self {
        self.key_deserializer = Some(NoopDeserializer::default());
        self
    }
}

impl<'a, K> ConsumerBuilder<'a, K, NoopDeserializer<()>>
where
    K: Deserializer,
{
    /// Sets the value serializer to empty
    pub fn without_value_deserializer(mut self) -> Self {
        self.value_deserializer = Some(NoopDeserializer::default());
        self
    }
}

impl<'a, K, V> ConsumerBuilder<'a, K, V>
where
    K: Deserializer,
    V: Deserializer,
    Self: 'static,
{
    /// Construct a `KafkaConsumer`
    pub fn build(self) -> Result<KafkaConsumer<'a, K, V>> {
        let client = if let Some(client) = self.client {
            client
        } else {
            KafkaClient::new(
                self.config.client.clone(),
                self.handle.ok_or(ErrorKind::ConfigError("missed handle"))?,
            )
        };

        Ok(KafkaConsumer::new(
            client,
            self.config,
            self.key_deserializer
                .ok_or(ErrorKind::ConfigError("missed key serializer"))?,
            self.value_deserializer
                .ok_or(ErrorKind::ConfigError("missed value serializer"))?,
        ))
    }
}

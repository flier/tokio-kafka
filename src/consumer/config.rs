use std::ops::{Deref, DerefMut};

use client::ClientConfig;

/// Configuration for the `KafkaConsumer`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    pub auto_commit: bool,

    /// The frequency in milliseconds that the consumer offsets are auto-committed to Kafka.
    #[serde(rename = "auto.commit.interval.ms")]
    pub auto_commit_interval: u64,
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

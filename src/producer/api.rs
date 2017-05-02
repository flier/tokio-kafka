use std::hash::Hash;

use protocol::{Offset, PartitionId, Timestamp};
use client::{StaticBoxFuture, TopicPartition};

/// A key/value pair to be sent to Kafka.
///
/// This consists of a topic name to which the record is being sent,
/// an optional partition number, and an optional key and value.
#[derive(Clone, Debug)]
pub struct ProducerRecord<'a, K, V>
    where K: Hash
{
    /// The topic this record is being sent to
    pub topic: &'a str,
    /// The partition to which the record will be sent (or `None` if no partition was specified)
    pub partition: Option<PartitionId>,
    /// The key (or `None` if no key is specified)
    pub key: Option<K>,
    /// The value
    pub value: V,
    /// The timestamp
    pub timestamp: Option<Timestamp>,
}

impl<'a, V> ProducerRecord<'a, (), V> {
    pub fn from_value(topic: &'a str, value: V) -> Self {
        ProducerRecord {
            topic: topic,
            partition: None,
            key: None,
            value: value,
            timestamp: None,
        }
    }
}

impl<'a, K, V> ProducerRecord<'a, K, V>
    where K: Hash
{
    pub fn from_key_value(topic: &'a str, key: K, value: V) -> Self {
        ProducerRecord {
            topic: topic,
            partition: None,
            key: Some(key),
            value: value,
            timestamp: None,
        }
    }

    pub fn with_partition(mut self, partition: PartitionId) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

/// The metadata for a record that has been acknowledged by the server
#[derive(Clone, Debug)]
pub struct RecordMetadata {
    /// The topic the record was appended to
    pub topic: String,
    /// The partition the record was sent to
    pub partition: PartitionId,
    /// The offset of the record in the topic/partition.
    pub offset: Offset,
    /// The timestamp of the record in the topic/partition.
    pub timestamp: Timestamp,
    /// The size of the serialized, uncompressed key in bytes.
    pub serialized_key_size: usize,
    /// The size of the serialized, uncompressed value in bytes.
    pub serialized_value_size: usize,
}

pub trait Producer {
    type Key: Hash;
    type Value;

    /// Get a list of partitions for the given topic for custom partition assignment.
    fn partitions_for(&self, topic: &str) -> Option<&[TopicPartition]>;

    /// Send the given record asynchronously and
    /// return a future which will eventually contain the response information.
    fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord;

    /// Flush any accumulated records from the producer.
    fn flush(&mut self) -> FlushProducer;
}

pub type SendRecord = StaticBoxFuture<RecordMetadata>;
pub type FlushProducer = StaticBoxFuture;
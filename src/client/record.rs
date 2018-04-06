use std::hash::Hash;

use protocol::{PartitionId, Timestamp};

/// A key/value pair to be sent to Kafka topic.
///
/// This consists of an optional partition number, and an optional key and value.
#[derive(Clone, Debug)]
pub struct TopicRecord<K, V> {
    /// The partition to which the record will be sent (or `None` if no partition was specified)
    pub partition_id: Option<PartitionId>,
    /// The key (or `None` if no key is specified)
    pub key: Option<K>,
    /// The value
    pub value: Option<V>,
    /// The timestamp
    pub timestamp: Option<Timestamp>,
}

impl<K> TopicRecord<K, ()>
where
    K: Hash,
{
    /// Creates a record to be sent to a specified topic with no value
    pub fn from_key(key: K) -> Self {
        TopicRecord {
            partition_id: None,
            key: Some(key),
            value: None,
            timestamp: None,
        }
    }
}

impl<V> TopicRecord<(), V> {
    /// Creates a record to be sent to a specified topic with no key
    pub fn from_value(value: V) -> Self {
        TopicRecord {
            partition_id: None,
            key: None,
            value: Some(value),
            timestamp: None,
        }
    }
}

impl<K, V> TopicRecord<K, V>
where
    K: Hash,
{
    /// Creates a record to be sent to a specified topic
    pub fn from_key_value(key: K, value: V) -> Self {
        TopicRecord {
            partition_id: None,
            key: Some(key),
            value: Some(value),
            timestamp: None,
        }
    }

    /// Creates a record with partition to be sent
    pub fn with_partition(mut self, partition_id: PartitionId) -> Self {
        self.partition_id = Some(partition_id);
        self
    }

    /// Creates a record with a specified timestamp to be sent
    pub fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    fn from_partition_record(partition_id: PartitionId, record: PartitionRecord<K, V>) -> TopicRecord<K, V> {
        TopicRecord {
            partition_id: Some(partition_id),
            key: record.key,
            value: record.value,
            timestamp: record.timestamp,
        }
    }
}

/// A key/value pair to be sent to Kafka partition.
#[derive(Clone, Debug)]
pub struct PartitionRecord<K, V> {
    /// The key (or `None` if no key is specified)
    pub key: Option<K>,
    /// The value
    pub value: Option<V>,
    /// The timestamp
    pub timestamp: Option<Timestamp>,
}

impl<K> PartitionRecord<K, ()>
where
    K: Hash,
{
    /// Creates a record to be sent to a specified topic and partition with no value
    pub fn from_key(key: K) -> Self {
        PartitionRecord {
            key: Some(key),
            value: None,
            timestamp: None,
        }
    }
}

impl<V> PartitionRecord<(), V> {
    /// Creates a record to be sent to a specified topic and partition with no key
    pub fn from_value(value: V) -> Self {
        PartitionRecord {
            key: None,
            value: Some(value),
            timestamp: None,
        }
    }
}

impl<K, V> PartitionRecord<K, V>
where
    K: Hash,
{
    /// Creates a record to be sent to a specified topic and partition
    pub fn from_key_value(key: K, value: V) -> Self {
        PartitionRecord {
            key: Some(key),
            value: Some(value),
            timestamp: None,
        }
    }

    /// Creates a record with a specified timestamp to be sent
    pub fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

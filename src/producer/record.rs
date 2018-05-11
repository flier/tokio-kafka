use std::borrow::Cow;
use std::hash::Hash;

use client::{PartitionRecord, TopicRecord};
use protocol::{Offset, PartitionId, RecordHeader, Timestamp};

/// A key/value pair to be sent to Kafka.
///
/// This consists of a topic name to which the record is being sent,
/// an optional partition number, and an optional key and value.
#[derive(Clone, Debug)]
pub struct ProducerRecord<K, V>
where
    K: Hash,
{
    /// The topic this record is being sent to
    pub topic_name: String,
    /// The partition to which the record will be sent (or `None` if no
    /// partition was specified)
    pub partition_id: Option<PartitionId>,
    /// The key (or `None` if no key is specified)
    pub key: Option<K>,
    /// The value
    pub value: Option<V>,
    /// The timestamp
    pub timestamp: Option<Timestamp>,
    /// The header
    pub headers: Vec<RecordHeader>,
}

impl<K> ProducerRecord<K, ()>
where
    K: Hash,
{
    /// Creates a record to be sent to a specified topic with no value
    pub fn from_key<S: AsRef<str>>(topic_name: S, key: K) -> Self {
        ProducerRecord {
            topic_name: topic_name.as_ref().to_owned(),
            partition_id: None,
            key: Some(key),
            value: None,
            timestamp: None,
            headers: Vec::new(),
        }
    }
}

impl<V> ProducerRecord<(), V> {
    /// Creates a record to be sent to a specified topic with no key
    pub fn from_value<S: AsRef<str>>(topic_name: S, value: V) -> Self {
        ProducerRecord {
            topic_name: topic_name.as_ref().to_owned(),
            partition_id: None,
            key: None,
            value: Some(value),
            timestamp: None,
            headers: Vec::new(),
        }
    }
}

impl<K, V> ProducerRecord<K, V>
where
    K: Hash,
{
    /// Creates a record to be sent to a specified topic
    pub fn from_key_value<S: AsRef<str>>(topic_name: S, key: K, value: V) -> Self {
        ProducerRecord {
            topic_name: topic_name.as_ref().to_owned(),
            partition_id: None,
            key: Some(key),
            value: Some(value),
            timestamp: None,
            headers: Vec::new(),
        }
    }

    pub fn from_partition_record<S: AsRef<str>>(
        topic_name: S,
        partition_id: Option<PartitionId>,
        record: PartitionRecord<K, V>,
    ) -> Self {
        ProducerRecord {
            topic_name: topic_name.as_ref().to_owned(),
            partition_id,
            key: record.key,
            value: record.value,
            timestamp: record.timestamp,
            headers: Vec::new(),
        }
    }

    pub fn from_topic_record<S: AsRef<str>>(topic_name: S, record: TopicRecord<K, V>) -> Self {
        ProducerRecord {
            topic_name: topic_name.as_ref().to_owned(),
            partition_id: record.partition_id,
            key: record.key,
            value: record.value,
            timestamp: record.timestamp,
            headers: Vec::new(),
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

    /// Add header to a record to be sent
    pub fn with_header<H>(mut self, header: H) -> Self
    where
        H: Into<RecordHeader>,
    {
        self.headers.push(header.into());
        self
    }

    /// Creates a record with headers to be sent
    pub fn with_headers<I, H>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = H>,
        H: Into<RecordHeader>,
    {
        self.headers = headers.into_iter().map(|h| h.into()).collect();
        self
    }
}

impl<'a> From<&'a str> for RecordHeader {
    fn from(key: &'a str) -> RecordHeader {
        RecordHeader {
            key: key.into(),
            value: None,
        }
    }
}

impl<'a> From<Cow<'a, str>> for RecordHeader {
    fn from(key: Cow<'a, str>) -> RecordHeader {
        RecordHeader {
            key: key.into_owned(),
            value: None,
        }
    }
}

impl From<String> for RecordHeader {
    fn from(key: String) -> RecordHeader {
        RecordHeader {
            key: key.into(),
            value: None,
        }
    }
}

/// The metadata for a record that has been acknowledged by the server
#[derive(Clone, Debug, Default)]
pub struct RecordMetadata {
    /// The topic the record was appended to
    pub topic_name: String,
    /// The partition the record was sent to
    pub partition_id: PartitionId,
    /// The offset of the record in the topic/partition.
    pub offset: Offset,
    /// The timestamp of the record in the topic/partition.
    pub timestamp: Timestamp,
    /// The size of the serialized, uncompressed key in bytes.
    pub serialized_key_size: usize,
    /// The size of the serialized, uncompressed value in bytes.
    pub serialized_value_size: usize,
}

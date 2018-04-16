#[macro_export]
macro_rules! topic_partition {
    ($topic_name: expr, $partition_id: expr) => {
        $crate::TopicPartition {
            topic_name: $topic_name.into(),
            partition_id: $partition_id,
        }
    };
    ($topic_name: expr, $partition_id: expr) => {
        topic_partition!($topic_name, 0)
    };
}

#[macro_export]
macro_rules! offset_and_metadata {
    ($offset: expr) => {
        $crate::OffsetAndMetadata {
            offset: $offset,
            metadata: None,
        }
    };
    ($offset: expr, $metadata: expr) => {
        $crate::OffsetAndMetadata {
            offset: $offset,
            metadata: Some($metadata.into()),
        }
    };
}

#[macro_export]
macro_rules! offset_and_timestamp {
    ($offset: expr) => {
        $crate::network::OffsetAndTimestamp {
            offset: $offset,
            timestamp: None,
        }
    };
    ($offset: expr, $timestamp: expr) => {
        $crate::network::OffsetAndTimestamp {
            offset: $offset,
            timestamp: Some($timestamp),
        }
    };
}

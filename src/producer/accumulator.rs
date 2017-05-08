use std::time::Duration;

use bytes::Bytes;

use compression::Compression;
use protocol::Timestamp;
use client::TopicPartition;

/// Accumulator acts as a queue that accumulates records
pub trait Accumulator {
    fn push(&mut self,
            tp: TopicPartition,
            timestamp: Timestamp,
            key: Option<Bytes>,
            value: Option<Bytes>)
            -> PushRecord;
}

/// RecordAccumulator acts as a queue that accumulates records into ProducerRecord instances to be sent to the server.
pub struct RecordAccumulator {
    /// The size to use when allocating ProducerRecord instances
    batch_size: usize,
    /// The maximum memory the record accumulator can use.
    total_size: usize,
    /// The compression codec for the records
    compression: Compression,
    /// An artificial delay time to add before declaring a records instance that isn't full ready for sending.
    ///
    /// This allows time for more records to arrive.
    /// Setting a non-zero lingerMs will trade off some latency for potentially better throughput
    /// due to more batching (and hence fewer, larger requests).
    linger: Duration,
    /// An artificial delay time to retry the produce request upon receiving an error.
    ///
    /// This avoids exhausting all retries in a short period of time.
    retry_backoff: Duration,
}

impl Accumulator for RecordAccumulator {
    fn push(&mut self,
            tp: TopicPartition,
            timestamp: Timestamp,
            key: Option<Bytes>,
            value: Option<Bytes>)
            -> PushRecord {
        PushRecord {}
    }
}

pub struct PushRecord {}

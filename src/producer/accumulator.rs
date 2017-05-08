use std::time::Duration;
use std::collections::{HashMap, VecDeque};

use bytes::Bytes;

use errors::Result;
use compression::Compression;
use protocol::{MessageSetBuilder, Timestamp};
use client::TopicPartition;

/// Accumulator acts as a queue that accumulates records
pub trait Accumulator<'a> {
    fn push_record(&mut self,
                   tp: TopicPartition<'a>,
                   timestamp: Timestamp,
                   key: Option<Bytes>,
                   value: Option<Bytes>)
                   -> Result<PushRecord>;
}

/// RecordAccumulator acts as a queue that accumulates records into ProducerRecord instances to be sent to the server.
pub struct RecordAccumulator<'a> {
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

    batches: HashMap<TopicPartition<'a>, VecDeque<ProducerBatch>>,
}

impl<'a> Accumulator<'a> for RecordAccumulator<'a> {
    fn push_record(&mut self,
                   tp: TopicPartition<'a>,
                   timestamp: Timestamp,
                   key: Option<Bytes>,
                   value: Option<Bytes>)
                   -> Result<PushRecord> {
        let batches = self.batches
            .entry(tp)
            .or_insert_with(|| VecDeque::new());

        if let Some(batch) = batches.back_mut() {
            if let Ok(push_record) = batch.try_push_record(timestamp, key.clone(), value.clone()) {
                return Ok(push_record);
            }
        }

        let mut batch = ProducerBatch::new();

        let push_recrod = batch.try_push_record(timestamp, key, value);

        batches.push_back(batch);

        push_recrod
    }
}

pub struct PushRecord {}

pub struct Thunk {}

pub struct ProducerBatch {
    builder: MessageSetBuilder,
    thunks: Vec<Thunk>,
}

impl ProducerBatch {
    pub fn new() -> Self {
        ProducerBatch {
            builder: MessageSetBuilder::new(),
            thunks: vec![],
        }
    }

    pub fn try_push_record(&mut self,
                           timestamp: Timestamp,
                           key: Option<Bytes>,
                           value: Option<Bytes>)
                           -> Result<PushRecord> {
        Ok(PushRecord {})
    }
}

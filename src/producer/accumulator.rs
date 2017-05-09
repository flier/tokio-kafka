use std::time::Duration;
use std::collections::{HashMap, VecDeque};

use bytes::Bytes;

use futures::future;

use errors::Result;
use compression::Compression;
use protocol::{ApiVersion, MessageSetBuilder, Timestamp, UsableApiVersion};
use client::{StaticBoxFuture, TopicPartition};
use producer::RecordMetadata;

/// Accumulator acts as a queue that accumulates records
pub trait Accumulator<'a> {
    /// Add a record to the accumulator, return the append result
    fn push_record(&mut self,
                   tp: TopicPartition<'a>,
                   timestamp: Timestamp,
                   key: Option<Bytes>,
                   value: Option<Bytes>)
                   -> PushRecord;
}

/// RecordAccumulator acts as a queue that accumulates records into ProducerRecord instances to be sent to the server.
pub struct RecordAccumulator<'a> {
    /// Request API versions for current connected brokers
    api_version: UsableApiVersion,
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
                   -> PushRecord {
        let batches = self.batches
            .entry(tp)
            .or_insert_with(|| VecDeque::new());

        if let Some(batch) = batches.back_mut() {
            let result = batch.push_record(timestamp, key.clone(), value.clone());

            if let Ok(push_recrod) = result {
                return push_recrod;
            }
        }

        let mut batch = ProducerBatch::new(self.api_version.max_version,
                                           self.compression,
                                           self.batch_size);

        let result = batch.push_record(timestamp, key, value);

        batches.push_back(batch);

        match result {
            Ok(push_recrod) => push_recrod,
            Err(err) => PushRecord::new(future::err(err)),
        }
    }
}

pub type PushRecord = StaticBoxFuture<RecordMetadata>;

pub struct Thunk {}

pub struct ProducerBatch {
    builder: MessageSetBuilder,
    thunks: Vec<Thunk>,
}

impl ProducerBatch {
    pub fn new(api_version: ApiVersion, compression: Compression, write_limit: usize) -> Self {
        ProducerBatch {
            builder: MessageSetBuilder::new(api_version, compression, write_limit, 0),
            thunks: vec![],
        }
    }

    pub fn push_record(&mut self,
                       timestamp: Timestamp,
                       key: Option<Bytes>,
                       value: Option<Bytes>)
                       -> Result<PushRecord> {
        self.builder.push(timestamp, key, value)?;

        Ok(PushRecord::new(future::ok(RecordMetadata::default())))
    }
}

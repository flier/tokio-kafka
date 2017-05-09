use std::time::{Duration, Instant};
use std::collections::{HashMap, VecDeque};

use bytes::Bytes;

use futures::{Future, Poll};
use futures::unsync::oneshot::{Canceled, Receiver, Sender, channel};

use errors::{Error, ErrorKind, Result};
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
                   -> Result<RecordAppendResult>;
}

/// RecordAccumulator acts as a queue that accumulates records into ProducerRecord instances to be sent to the server.
pub struct RecordAccumulator<'a> {
    /// Request API versions for current connected brokers
    pub api_version: UsableApiVersion,
    /// The size to use when allocating ProducerRecord instances
    pub batch_size: usize,
    /// The maximum memory the record accumulator can use.
    pub total_size: usize,
    /// The compression codec for the records
    pub compression: Compression,
    /// An artificial delay time to add before declaring a records instance that isn't full ready for sending.
    ///
    /// This allows time for more records to arrive.
    /// Setting a non-zero lingerMs will trade off some latency for potentially better throughput
    /// due to more batching (and hence fewer, larger requests).
    pub linger: Duration,
    /// An artificial delay time to retry the produce request upon receiving an error.
    ///
    /// This avoids exhausting all retries in a short period of time.
    pub retry_backoff: Duration,

    pub batches: HashMap<TopicPartition<'a>, VecDeque<ProducerBatch>>,
}

impl<'a> RecordAccumulator<'a> {}

pub struct RecordAppendResult {
    pub produce_record: PushRecord,
    pub batch_is_full: bool,
    pub new_batch_created: bool,
}

impl<'a> Accumulator<'a> for RecordAccumulator<'a> {
    fn push_record(&mut self,
                   tp: TopicPartition<'a>,
                   timestamp: Timestamp,
                   key: Option<Bytes>,
                   value: Option<Bytes>)
                   -> Result<RecordAppendResult> {
        let batches = self.batches
            .entry(tp)
            .or_insert_with(|| VecDeque::new());

        let batches_len = batches.len();

        if let Some(batch) = batches.back_mut() {
            let result = batch.push_record(timestamp, key.clone(), value.clone());

            if let Ok(push_recrod) = result {
                return Ok(RecordAppendResult {
                              produce_record: PushRecord::new(push_recrod),
                              batch_is_full: batches_len > 1 || batch.is_full(),
                              new_batch_created: false,
                          });
            }
        }

        let mut batch = ProducerBatch::new(self.api_version.max_version,
                                           self.compression,
                                           self.batch_size);

        let result = batch.push_record(timestamp, key, value);

        let batch_is_full = batch.is_full();

        batches.push_back(batch);

        result.map(move |push_recrod| {
                       RecordAppendResult {
                           produce_record: PushRecord::new(push_recrod),
                           batch_is_full: batches_len > 0 || batch_is_full,
                           new_batch_created: true,
                       }
                   })
    }
}

pub type PushRecord = StaticBoxFuture<RecordMetadata>;

pub struct Thunk {
    sender: Sender<RecordMetadata>,
}

pub struct ProducerBatch {
    builder: MessageSetBuilder,
    thunks: Vec<Thunk>,
    create_time: Instant,
    last_push_time: Instant,
}

impl ProducerBatch {
    pub fn new(api_version: ApiVersion, compression: Compression, write_limit: usize) -> Self {
        let now = Instant::now();

        ProducerBatch {
            builder: MessageSetBuilder::new(api_version, compression, write_limit, 0),
            thunks: vec![],
            create_time: now,
            last_push_time: now,
        }
    }

    pub fn is_full(&self) -> bool {
        self.builder.is_full()
    }

    pub fn push_record(&mut self,
                       timestamp: Timestamp,
                       key: Option<Bytes>,
                       value: Option<Bytes>)
                       -> Result<PushRecordResult> {
        self.builder.push(timestamp, key, value)?;

        let (sender, receiver) = channel();

        self.thunks.push(Thunk { sender: sender });
        self.last_push_time = Instant::now();

        Ok(PushRecordResult { receiver: receiver })
    }
}

pub struct PushRecordResult {
    receiver: Receiver<RecordMetadata>,
}

impl Future for PushRecordResult {
    type Item = RecordMetadata;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.receiver.poll() {
            Ok(result) => Ok(result),
            Err(Canceled) => bail!(ErrorKind::Canceled),
        }
    }
}

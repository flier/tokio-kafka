use std::rc::Rc;
use std::cell::RefCell;
use std::ops::Deref;
use std::time::{Duration, Instant};
use std::collections::{HashMap, VecDeque};

use bytes::Bytes;

use futures::{Async, Future, Poll, Stream, future};
use futures::unsync::oneshot::{Canceled, Receiver, Sender, channel};

use errors::{Error, ErrorKind, Result};
use compression::Compression;
use protocol::{ApiVersion, KafkaCode, MessageSet, MessageSetBuilder, Offset, PartitionId,
               Timestamp};
use network::TopicPartition;
use client::StaticBoxFuture;
use producer::RecordMetadata;

/// Accumulator acts as a queue that accumulates records
pub trait Accumulator<'a> {
    /// Add a record to the accumulator, return the append result
    fn push_record(&mut self,
                   tp: TopicPartition<'a>,
                   timestamp: Timestamp,
                   key: Option<Bytes>,
                   value: Option<Bytes>,
                   api_version: ApiVersion)
                   -> PushRecord;

    fn flush(&mut self) -> Result<()>;
}

/// RecordAccumulator acts as a queue that accumulates records into ProducerRecord instances to be sent to the server.
pub struct RecordAccumulator<'a> {
    /// The size to use when allocating ProducerRecord instances
    batch_size: usize,
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

    batches: Rc<RefCell<HashMap<TopicPartition<'a>, VecDeque<ProducerBatch>>>>,
}

impl<'a> RecordAccumulator<'a> {
    pub fn new(batch_size: usize,
               compression: Compression,
               linger: Duration,
               retry_backoff: Duration)
               -> Self {
        RecordAccumulator {
            batch_size: batch_size,
            compression: compression,
            linger: linger,
            retry_backoff: retry_backoff,
            batches: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn batches(&self) -> Batches<'a> {
        Batches { batches: self.batches.clone() }
    }
}

impl<'a> Accumulator<'a> for RecordAccumulator<'a> {
    fn push_record(&mut self,
                   tp: TopicPartition<'a>,
                   timestamp: Timestamp,
                   key: Option<Bytes>,
                   value: Option<Bytes>,
                   api_version: ApiVersion)
                   -> PushRecord {
        let mut batches = self.batches.borrow_mut();
        let batches = batches.entry(tp).or_insert_with(|| VecDeque::new());

        if let Some(batch) = batches.back_mut() {
            if let Ok(push_recrod) = batch.push_record(timestamp, key.clone(), value.clone()) {
                return PushRecord::new(push_recrod);
            }
        }

        let mut batch = ProducerBatch::new(api_version, self.compression, self.batch_size);

        match batch.push_record(timestamp, key, value) {
            Ok(push_recrod) => {
                batches.push_back(batch);

                PushRecord::new(push_recrod)
            }
            Err(err) => PushRecord::new(future::err(err)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        for (_, batches) in self.batches.borrow_mut().iter_mut() {
            let api_version = batches.back().map(|batch| batch.api_version());

            if let Some(api_version) = api_version {
                batches.push_back(ProducerBatch::new(api_version,
                                                     self.compression,
                                                     self.batch_size))
            }
        }

        Ok(())
    }
}

pub type PushRecord = StaticBoxFuture<RecordMetadata>;
pub type Flush = StaticBoxFuture;

pub struct Batches<'a> {
    batches: Rc<RefCell<HashMap<TopicPartition<'a>, VecDeque<ProducerBatch>>>>,
}

impl<'a> Stream for Batches<'a> {
    type Item = (TopicPartition<'a>, ProducerBatch);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        for (tp, batches) in self.batches.borrow_mut().iter_mut() {
            let is_full = batches.len() > 1 ||
                          batches.back().map_or(false, |batches| batches.is_full());

            if is_full {
                if let Some(batch) = batches.pop_front() {
                    return Ok(Async::Ready(Some((tp.clone(), batch))));
                }
            }
        }

        Ok(Async::NotReady)
    }
}

pub struct Thunk {
    sender: Sender<Result<RecordMetadata>>,
    relative_offset: Offset,
    timestamp: Timestamp,
    key_size: usize,
    value_size: usize,
}

impl Thunk {
    pub fn done(self,
                topic_name: &str,
                partition: PartitionId,
                base_offset: Offset,
                error_code: KafkaCode)
                -> ::std::result::Result<(), Result<RecordMetadata>> {
        let result = if error_code == KafkaCode::None {
            Ok(RecordMetadata {
                   topic_name: topic_name.to_owned(),
                   partition: partition,
                   offset: base_offset + self.relative_offset,
                   timestamp: self.timestamp,
                   serialized_key_size: self.key_size,
                   serialized_value_size: self.value_size,
               })
        } else {
            Err(ErrorKind::KafkaError(error_code).into())
        };

        self.sender.send(result)
    }
}

pub struct ProducerBatch {
    builder: MessageSetBuilder,
    thunks: Vec<Thunk>,
    create_time: Instant,
    last_push_time: Instant,
}

impl Deref for ProducerBatch {
    type Target = MessageSetBuilder;

    fn deref(&self) -> &Self::Target {
        &self.builder
    }
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

    pub fn push_record(&mut self,
                       timestamp: Timestamp,
                       key: Option<Bytes>,
                       value: Option<Bytes>)
                       -> Result<FutureRecordMetadata> {
        let key_size = key.as_ref().map_or(0, |b| b.len());
        let value_size = value.as_ref().map_or(0, |b| b.len());

        let relative_offset = self.builder.push(timestamp, key, value)?;

        let (sender, receiver) = channel();

        self.thunks
            .push(Thunk {
                      sender: sender,
                      relative_offset: relative_offset,
                      timestamp: timestamp,
                      key_size: key_size,
                      value_size: value_size,
                  });
        self.last_push_time = Instant::now();

        Ok(FutureRecordMetadata { receiver: receiver })
    }

    pub fn build(self) -> (Vec<Thunk>, MessageSet) {
        (self.thunks, self.builder.build())
    }
}

pub struct FutureRecordMetadata {
    receiver: Receiver<Result<RecordMetadata>>,
}

impl Future for FutureRecordMetadata {
    type Item = RecordMetadata;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.receiver.poll() {
            Ok(Async::Ready(Ok(metadata))) => Ok(Async::Ready(metadata)),
            Ok(Async::Ready(Err(err))) => Err(err),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(Canceled) => bail!(ErrorKind::Canceled),
        }
    }
}

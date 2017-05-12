use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;
use std::collections::{HashMap, VecDeque};

use bytes::Bytes;

use futures::{Async, Poll, Stream, future};

use errors::Error;
use compression::Compression;
use protocol::{ApiVersion, Timestamp};
use network::TopicPartition;
use client::StaticBoxFuture;
use producer::{ProducerBatch, RecordMetadata};

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

    fn flush(&mut self);
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
            match batch.push_record(timestamp, key.clone(), value.clone()) {
                Ok(push_recrod) => {
                    trace!("pushed record to latest batch, {:?}", batch);

                    return PushRecord::new(push_recrod);
                }
                Err(err) => {
                    trace!("fail to push record, {}", err);
                }
            }
        }

        let mut batch = ProducerBatch::new(api_version, self.compression, self.batch_size);

        match batch.push_record(timestamp, key, value) {
            Ok(push_recrod) => {
                trace!("pushed record to a new batch, {:?}", batch);

                batches.push_back(batch);

                PushRecord::new(push_recrod)
            }
            Err(err) => PushRecord::new(future::err(err)),
        }
    }

    fn flush(&mut self) {
        for (_, batches) in self.batches.borrow_mut().iter_mut() {
            let api_version = batches.back().map(|batch| batch.api_version());

            if let Some(api_version) = api_version {
                batches.push_back(ProducerBatch::new(api_version,
                                                     self.compression,
                                                     self.batch_size))
            }
        }
    }
}

pub type PushRecord = StaticBoxFuture<RecordMetadata>;

pub struct Batches<'a> {
    batches: Rc<RefCell<HashMap<TopicPartition<'a>, VecDeque<ProducerBatch>>>>,
}

impl<'a> Stream for Batches<'a> {
    type Item = (TopicPartition<'a>, ProducerBatch);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        for (tp, batches) in self.batches.borrow_mut().iter_mut() {
            let is_full = batches.len() > 1 ||
                          batches.back().map_or(false, |batch| batch.is_full());

            if is_full {
                if let Some(batch) = batches.pop_front() {
                    trace!("batch is ready to send, {:?}", batch);

                    return Ok(Async::Ready(Some((tp.clone(), batch))));
                }
            }
        }

        Ok(Async::NotReady)
    }
}

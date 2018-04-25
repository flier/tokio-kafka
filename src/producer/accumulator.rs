use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;

use futures::{Async, Future, IntoFuture, Poll, Stream};

use client::{StaticBoxFuture, ToStaticBoxFuture};
use compression::Compression;
use errors::Error;
use network::TopicPartition;
use producer::{ProducerBatch, RecordMetadata};
use protocol::{RecordFormat, RecordHeader, Timestamp};

/// Accumulator acts as a queue that accumulates records
pub trait Accumulator<'a> {
    /// Add a record to the accumulator, return the append result
    fn push_record(
        &self,
        tp: TopicPartition<'a>,
        timestamp: Timestamp,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<RecordHeader>,
        record_format: RecordFormat,
    ) -> PushRecord;

    fn flush(&mut self);
}

/// `RecordAccumulator` acts as a queue that accumulates records into `ProducerRecord` instances to
/// be sent to the server.
pub struct RecordAccumulator<'a> {
    /// The size to use when allocating ProducerRecord instances
    batch_size: usize,

    /// The compression codec for the records
    compression: Compression,

    /// An artificial delay time to add before declaring a records instance that isn't full ready
    /// for sending.
    ///
    /// This allows time for more records to arrive.
    /// Setting a non-zero lingerMs will trade off some latency for potentially better throughput
    /// due to more batching (and hence fewer, larger requests).
    linger: Duration,

    batches: Rc<RefCell<HashMap<TopicPartition<'a>, VecDeque<ProducerBatch>>>>,
}

impl<'a> RecordAccumulator<'a> {
    pub fn new(batch_size: usize, compression: Compression, linger: Duration) -> Self {
        RecordAccumulator {
            batch_size,
            compression,
            linger,
            batches: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn batches(&self, force: bool) -> Batches<'a> {
        Batches {
            batches: self.batches.clone(),
            linger: self.linger,
            force,
        }
    }
}

impl<'a> Accumulator<'a> for RecordAccumulator<'a> {
    fn push_record(
        &self,
        tp: TopicPartition<'a>,
        timestamp: Timestamp,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<RecordHeader>,
        record_format: RecordFormat,
    ) -> PushRecord {
        let mut batches = self.batches.borrow_mut();
        let batches = batches.entry(tp).or_insert_with(VecDeque::new);

        if let Some(batch) = batches.back_mut() {
            match batch.push_record(timestamp, key.clone(), value.clone(), headers.clone()) {
                Ok(push_recrod) => {
                    trace!("pushed record to latest batch, {:?}", batch);

                    return PushRecord::new(push_recrod, batch.is_full(), false);
                }
                Err(err) => {
                    trace!("fail to push record to batch that exceeded limits, try again, {}", err);
                }
            }
        }

        let mut batch = ProducerBatch::new(record_format, self.compression, self.batch_size);

        match batch.push_record(timestamp, key, value, headers) {
            Ok(push_recrod) => {
                trace!("pushed record to a new batch, {:?}", batch);

                let batch_is_full = batch.is_full();

                batches.push_back(batch);

                PushRecord::new(push_recrod, batch_is_full, true)
            }
            Err(err) => {
                warn!("fail to push record, {}", err);

                PushRecord::new(Err(err), false, true)
            }
        }
    }

    fn flush(&mut self) {
        trace!("flush all batches");

        for (_, batches) in self.batches.borrow_mut().iter_mut() {
            if let Some(record_format) = batches.back().map(|batch| batch.record_format()) {
                batches.push_back(ProducerBatch::new(record_format, self.compression, self.batch_size))
            }
        }
    }
}

pub struct PushRecord {
    future: StaticBoxFuture<RecordMetadata>,
    is_full: bool,
    new_batch: bool,
}

impl PushRecord {
    pub fn new<F>(future: F, is_full: bool, new_batch: bool) -> Self
    where
        F: IntoFuture<Item = RecordMetadata, Error = Error> + 'static,
    {
        PushRecord {
            future: future.static_boxed(),
            is_full,
            new_batch,
        }
    }

    pub fn is_full(&self) -> bool {
        self.is_full
    }

    pub fn new_batch(&self) -> bool {
        self.new_batch
    }
}

impl Future for PushRecord {
    type Item = RecordMetadata;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.future.poll()
    }
}

pub struct Batches<'a> {
    batches: Rc<RefCell<HashMap<TopicPartition<'a>, VecDeque<ProducerBatch>>>>,
    linger: Duration,
    force: bool,
}

impl<'a> Stream for Batches<'a> {
    type Item = (TopicPartition<'a>, ProducerBatch);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        for (tp, batches) in self.batches.borrow_mut().iter_mut() {
            let ready = self.force || batches.back().map_or(false, |batch| {
                batch.is_full() || batch.create_time().elapsed() >= self.linger
            });

            if ready {
                if let Some(batch) = batches.pop_front() {
                    return Ok(Async::Ready(Some((tp.clone(), batch))));
                }
            }
        }

        Ok(Async::NotReady)
    }
}

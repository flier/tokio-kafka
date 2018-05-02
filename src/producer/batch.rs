use std::cell::RefCell;
use std::hash::Hash;
use std::ops::Deref;
use std::rc::Rc;
use std::time::Instant;

use bytes::{BigEndian, Bytes};

use futures::unsync::oneshot::{channel, Canceled, Receiver, Sender};
use futures::{Async, Future, Poll};

use compression::Compression;
use errors::{Error, ErrorKind, Result};
use producer::{ProducerInterceptor, ProducerInterceptors, RecordMetadata};
use protocol::{KafkaCode, MessageSet, MessageSetBuilder, Offset, PartitionId, RecordFormat, RecordHeader, Timestamp};

#[derive(Debug)]
pub struct Thunk {
    sender: Sender<Result<RecordMetadata>>,
    relative_offset: Offset,
    timestamp: Timestamp,
    key_size: usize,
    value_size: usize,
}

impl Thunk {
    pub fn fail(self, err: Error) -> ::std::result::Result<(), Result<RecordMetadata>> {
        self.sender.send(Err(err))
    }

    pub fn done<K: Hash, V>(
        self,
        interceptors: Option<Rc<RefCell<ProducerInterceptors<K, V>>>>,
        topic_name: &str,
        partition_id: PartitionId,
        base_offset: Offset,
        error_code: KafkaCode,
    ) -> ::std::result::Result<(), Result<RecordMetadata>> {
        let result = if error_code == KafkaCode::None {
            Ok(RecordMetadata {
                topic_name: topic_name.to_owned(),
                partition_id,
                offset: base_offset + self.relative_offset,
                timestamp: self.timestamp,
                serialized_key_size: self.key_size,
                serialized_value_size: self.value_size,
            })
        } else {
            Err(ErrorKind::KafkaError(error_code).into())
        };

        if let Some(interceptors) = interceptors {
            (*interceptors).borrow().ack(&result);
        }

        self.sender.send(result)
    }
}

#[derive(Debug)]
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
    pub fn new(record_format: RecordFormat, compression: Compression, write_limit: usize) -> Self {
        let now = Instant::now();

        ProducerBatch {
            builder: MessageSetBuilder::new(record_format, compression, write_limit, 0),
            thunks: vec![],
            create_time: now,
            last_push_time: now,
        }
    }

    pub fn create_time(&self) -> &Instant {
        &self.create_time
    }

    pub fn last_push_time(&self) -> &Instant {
        &self.last_push_time
    }

    pub fn push_record(
        &mut self,
        timestamp: Timestamp,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<RecordHeader>,
    ) -> Result<FutureRecordMetadata> {
        let key_size = key.as_ref().map_or(0, |b| b.len());
        let value_size = value.as_ref().map_or(0, |b| b.len());

        let relative_offset = self.builder.push(timestamp, key, value, headers)?;

        let (sender, receiver) = channel();

        self.thunks.push(Thunk {
            sender,
            relative_offset,
            timestamp,
            key_size,
            value_size,
        });
        self.last_push_time = Instant::now();

        Ok(FutureRecordMetadata { receiver })
    }

    pub fn build(self) -> Result<(Vec<Thunk>, MessageSet)> {
        Ok((self.thunks, self.builder.build::<BigEndian>()?))
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
            Err(Canceled) => bail!(ErrorKind::Canceled("produce record")),
        }
    }
}

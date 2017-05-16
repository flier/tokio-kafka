#![recursion_limit="128"]

#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]
#![cfg_attr(feature="clippy", allow(module_inception, block_in_if_condition_stmt))]

#![allow(dead_code)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate lazy_static;
extern crate bytes;
#[macro_use]
extern crate nom;
extern crate crc;
extern crate twox_hash;
extern crate time;
extern crate hexplay;
extern crate encoding;
extern crate byteorder;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate prometheus;

extern crate futures;
extern crate futures_cpupool;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;
extern crate tokio_middleware;
extern crate tokio_timer;
extern crate tokio_retry;
extern crate tokio_tls;
extern crate native_tls;

#[cfg(feature = "gzip")]
extern crate flate2;

#[cfg(feature = "snappy")]
extern crate snap;

#[cfg(feature = "lz4")]
extern crate lz4_compress;

#[cfg(test)]
extern crate pretty_env_logger;

#[macro_use]
pub mod errors;
mod compression;
#[macro_use]
mod protocol;
mod network;
mod client;
mod producer;
mod consumer;

pub mod consts {
    pub use client::{DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS, DEFAULT_REQUEST_TIMEOUT_MILLS};
    pub use producer::{DEFAULT_ACK_TIMEOUT_MILLIS, DEFAULT_BATCH_SIZE, DEFAULT_LINGER_MILLIS,
                       DEFAULT_MAX_REQUEST_SIZE};
}

pub use errors::{Error, ErrorKind, Result};
pub use compression::Compression;
pub use protocol::{FetchOffset, PartitionId, RequiredAcks, ToMilliseconds};
pub use network::TopicPartition;
pub use client::{Broker, BrokerRef, Client, ClientConfig, Cluster, KafkaClient, KafkaVersion,
                 Metadata, PartitionOffset, StaticBoxFuture};
pub use producer::{BytesSerializer, DefaultPartitioner, KafkaProducer, NoopSerializer,
                   Partitioner, Producer, ProducerBuilder, ProducerConfig, ProducerInterceptor,
                   ProducerRecord, RawSerializer, RecordMetadata, Serializer,
                   StrEncodingSerializer};
pub use consumer::KafkaConsumer;

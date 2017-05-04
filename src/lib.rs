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

extern crate serde;
#[macro_use]
extern crate serde_derive;

extern crate futures;
extern crate futures_cpupool;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;
extern crate tokio_timer;
extern crate tokio_tls;
extern crate native_tls;

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
    pub use client::{DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS, DEFAULT_MAX_POOLED_CONNECTION};
    pub use producer::{DEFAULT_ACK_TIMEOUT_MILLIS, DEFAULT_BATCH_SIZE, DEFAULT_MAX_REQUEST_SIZE};
}

pub use errors::{Error, ErrorKind};
pub use compression::Compression;
pub use protocol::{FetchOffset, PartitionId, RequiredAcks};
pub use client::{Broker, BrokerRef, Client, Cluster, KafkaClient, KafkaConfig, Metadata,
                 PartitionOffset, StaticBoxFuture, TopicPartition};
pub use producer::{KafkaProducer, Partitioner, Producer, ProducerBuilder, ProducerConfig,
                   ProducerRecord};
pub use consumer::KafkaConsumer;
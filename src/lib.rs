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

pub use compression::Compression;
pub use protocol::{FetchOffset, PartitionId, RequiredAcks};
pub use client::{Broker, BrokerRef, Client, KafkaClient, KafkaConfig, KafkaOption, Metadata,
                 PartitionOffset, TopicPartition, TopicPartitions};
pub use producer::{KafkaProducer, Partitioner, Producer, ProducerBuilder, ProducerConfig,
                   ProducerRecord};
pub use consumer::KafkaConsumer;
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
extern crate xxhash2;
extern crate time;
extern crate hexplay;

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

pub mod errors;
mod codec;
mod compression;
mod protocol;
mod network;
mod client;
mod producer;
mod consumer;

pub use compression::Compression;
pub use protocol::RequiredAcks;
pub use client::{KafkaConfig, KafkaClient};
pub use producer::KafkaProducer;
pub use consumer::KafkaConsumer;
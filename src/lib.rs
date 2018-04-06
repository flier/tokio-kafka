#![recursion_limit = "128"]
#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy(conf_file = ".clippy.toml")))]
#![cfg_attr(feature = "clippy", allow(module_inception, block_in_if_condition_stmt))]
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
extern crate byteorder;
extern crate crc;
#[cfg(feature = "encoding")]
extern crate encoding;
extern crate hexplay;
extern crate rand;
extern crate serde;
extern crate time;
extern crate twox_hash;
#[macro_use]
extern crate serde_derive;
#[cfg(feature = "json")]
extern crate serde_json;
#[macro_use]
extern crate prometheus;

extern crate futures;
extern crate futures_cpupool;
extern crate native_tls;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_retry;
extern crate tokio_service;
extern crate tokio_timer;
extern crate tokio_tls;

#[cfg(feature = "gzip")]
extern crate flate2;

#[cfg(feature = "snappy")]
extern crate snap;

#[cfg(feature = "lz4")]
extern crate lz4_compress;

#[cfg(test)]
extern crate pretty_env_logger;
#[cfg(test)]
extern crate typemap;

#[macro_use]
mod errors;
mod compression;
#[macro_use]
mod protocol;
mod serialization;
#[macro_use]
mod network;
mod client;
mod consumer;
mod producer;

pub use client::{Broker, BrokerRef, Client, ClientBuilder, ClientConfig, Cluster, KafkaClient, KafkaVersion,
                 ListOffsets, ListedOffset, LoadMetadata, Metadata, PartitionRecord, ProduceRecords,
                 ToStaticBoxFuture, TopicRecord, DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS,
                 DEFAULT_METADATA_MAX_AGE_MILLS, DEFAULT_REQUEST_TIMEOUT_MILLS, DEFAULT_RETRY_BACKOFF_MILLIS};
pub use compression::Compression;
pub use consumer::{Consumer, ConsumerBuilder, KafkaConsumer, SeekTo, Subscribed};
pub use errors::{Error, ErrorKind, Result};
pub use network::{OffsetAndMetadata, OffsetAndTimestamp, TopicPartition};
pub use producer::{DefaultPartitioner, GetTopic, KafkaProducer, Partitioner, Producer, ProducerBuilder,
                   ProducerConfig, ProducerInterceptor, ProducerPartition, ProducerRecord, ProducerTopic,
                   RecordMetadata, SendRecord, DEFAULT_ACK_TIMEOUT_MILLIS, DEFAULT_BATCH_SIZE, DEFAULT_LINGER_MILLIS,
                   DEFAULT_MAX_REQUEST_SIZE};
pub use protocol::{ApiKey, ApiKeys, ErrorCode, FetchOffset, KafkaCode, Offset, PartitionId, RequiredAcks, Timestamp,
                   ToMilliseconds, UsableApiVersion, UsableApiVersions};
pub use serialization::{BytesDeserializer, BytesSerializer, Deserializer, NoopDeserializer, NoopSerializer,
                        RawDeserializer, RawSerializer, Serializer, StringDeserializer, StringSerializer};
#[cfg(feature = "json")]
pub use serialization::{JsonDeserializer, JsonSerializer};
#[cfg(feature = "encoding")]
pub use serialization::{StrEncodingDeserializer, StrEncodingSerializer};

#[macro_export]
macro_rules! topic_partition {
    ($topic_name:expr, $partition_id:expr) => {
        $crate::TopicPartition {
            topic_name: $topic_name.into(),
            partition_id: $partition_id,
        }
    };
}

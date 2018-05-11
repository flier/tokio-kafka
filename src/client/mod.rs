mod builder;
mod client;
mod cluster;
mod config;
mod metadata;
mod metrics;
mod middleware;
mod record;
mod service;
mod version;

#[cfg(test)]
mod mock;

pub use self::builder::ClientBuilder;
pub use self::client::{simple_timeout, Client, ConsumerGroup, ConsumerGroupAssignment, ConsumerGroupMember,
                       ConsumerGroupProtocol, FetchRecords, FetchedRecords, Generation, GetMetadata, GroupCoordinator,
                       Heartbeat, JoinGroup, KafkaClient, LeaveGroup, ListOffsets, ListedOffset, LoadMetadata,
                       OffsetCommit, OffsetFetch, PartitionData, ProduceRecords, StaticBoxFuture, SyncGroup,
                       ToStaticBoxFuture};
pub use self::cluster::{Broker, BrokerRef, Cluster, PartitionInfo};
pub use self::config::{ClientConfig, DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS, DEFAULT_METADATA_MAX_AGE_MILLS,
                       DEFAULT_REQUEST_TIMEOUT_MILLS, DEFAULT_RETRY_BACKOFF_MILLIS};
pub use self::metadata::{Metadata, TopicPartitions};
pub use self::metrics::Metrics;
pub use self::middleware::InFlightMiddleware;
pub use self::record::{PartitionRecord, TopicRecord};
pub use self::service::{FutureResponse, KafkaService};
pub use self::version::KafkaVersion;

#[cfg(test)]
pub use self::mock::MockClient;

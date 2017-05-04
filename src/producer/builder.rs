use std::time::Duration;
use std::ops::{Deref, DerefMut};
use std::net::SocketAddr;

use tokio_core::reactor::Handle;

use errors::{ErrorKind, Result};
use compression::Compression;
use protocol::RequiredAcks;
use client::KafkaClient;
use producer::{DefaultPartitioner, KafkaProducer, ProducerConfig};

pub struct ProducerBuilder<'a, K, V, P = DefaultPartitioner> {
    config: ProducerConfig,
    client: Option<KafkaClient<'a>>,
    handle: Option<Handle>,
    partitioner: P,
    key_serializer: Option<K>,
    value_serializer: Option<V>,
}

impl<'a, K, V, P> Deref for ProducerBuilder<'a, K, V, P> {
    type Target = ProducerConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl<'a, K, V, P> DerefMut for ProducerBuilder<'a, K, V, P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl<'a, K, V> Default for ProducerBuilder<'a, K, V> {
    fn default() -> Self {
        ProducerBuilder {
            config: ProducerConfig::default(),
            client: None,
            handle: None,
            partitioner: DefaultPartitioner::default(),
            key_serializer: None,
            value_serializer: None,
        }
    }
}

impl<'a, K, V> ProducerBuilder<'a, K, V, DefaultPartitioner> {
    pub fn new() -> Self {
        ProducerBuilder::default()
    }

    pub fn from_client(client: KafkaClient<'a>,
                       config: ProducerConfig)
                       -> ProducerBuilder<'a, K, V, DefaultPartitioner> {
        ProducerBuilder {
            client: Some(client),
            handle: None,
            config: config,
            partitioner: DefaultPartitioner::default(),
            key_serializer: None,
            value_serializer: None,
        }
    }

    pub fn from_config(config: ProducerConfig,
                       handle: Handle)
                       -> ProducerBuilder<'a, K, V, DefaultPartitioner> {
        ProducerBuilder {
            client: None,
            handle: Some(handle),
            config: config,
            partitioner: DefaultPartitioner::default(),
            key_serializer: None,
            value_serializer: None,
        }
    }

    pub fn from_hosts<I>(hosts: I, handle: Handle) -> Self
        where I: Iterator<Item = SocketAddr>
    {
        Self::from_config(ProducerConfig::from_hosts(hosts), handle)
    }
}

trait ToMilliseconds {
    fn as_millis(&self) -> u64;
}

impl ToMilliseconds for Duration {
    fn as_millis(&self) -> u64 {
        self.as_secs() * 1000 + self.subsec_nanos() as u64 / 1000_000
    }
}

impl<'a, K, V, P> ProducerBuilder<'a, K, V, P> {
    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.config.client_id = Some(client_id);
        self
    }

    pub fn with_max_connection_idle(mut self, max_connection_idle: Duration) -> Self {
        self.config.max_connection_idle = max_connection_idle.as_millis();
        self
    }

    pub fn with_required_acks(mut self, acks: RequiredAcks) -> Self {
        self.config.acks = acks;
        self
    }

    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.batch_size = batch_size;
        self
    }

    pub fn with_ack_timeout(mut self, ack_timeout: Duration) -> Self {
        self.config.ack_timeout = ack_timeout.as_millis();
        self
    }

    pub fn with_client(mut self, client: KafkaClient<'a>) -> Self {
        self.client = Some(client);
        self
    }

    pub fn with_handle(mut self, handle: Handle) -> Self {
        self.handle = Some(handle);
        self
    }

    pub fn with_partitioner(mut self, partitioner: P) -> Self {
        self.partitioner = partitioner;
        self
    }

    pub fn with_key_serializer(mut self, key_serializer: K) -> Self {
        self.key_serializer = Some(key_serializer);
        self
    }

    pub fn with_value_serializer(mut self, value_serializer: V) -> Self {
        self.value_serializer = Some(value_serializer);
        self
    }
}

impl<'a, K, V, P> ProducerBuilder<'a, K, V, P>
    where Self: 'static
{
    pub fn build(self) -> Result<KafkaProducer<'a, K, V>> {
        let client = if let Some(client) = self.client {
            client
        } else if let Some(handle) = self.handle {
            KafkaClient::from_config(self.config.client.clone(), handle)
        } else {
            bail!(ErrorKind::ConfigError("missed client or handle"))
        };

        Ok(KafkaProducer::from_client(client, self.config))
    }
}
use std::rc::Rc;
use std::cell::RefCell;
use std::hash::Hash;
use std::time::Duration;
use std::ops::{Deref, DerefMut};
use std::net::SocketAddr;

use tokio_core::reactor::Handle;

use errors::{ErrorKind, Result};
use compression::Compression;
use protocol::{RequiredAcks, ToMilliseconds};
use serialization::{NoopSerializer, Serializer};
use client::{KafkaClient, KafkaVersion};
use producer::{DefaultPartitioner, Interceptors, KafkaProducer, ProducerConfig,
               ProducerInterceptor, ProducerInterceptors};

/// A `KafkaProducer` builder easing the process of setting up various configuration settings.
pub struct ProducerBuilder<'a, K, V, P = DefaultPartitioner>
    where K: Serializer,
          V: Serializer
{
    config: ProducerConfig,
    client: Option<KafkaClient<'a>>,
    handle: Option<Handle>,
    key_serializer: Option<K>,
    value_serializer: Option<V>,
    partitioner: Option<P>,
    interceptors: Interceptors<K::Item, V::Item>,
}

impl<'a, K, V, P> Deref for ProducerBuilder<'a, K, V, P>
    where K: Serializer,
          V: Serializer
{
    type Target = ProducerConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl<'a, K, V, P> DerefMut for ProducerBuilder<'a, K, V, P>
    where K: Serializer,
          V: Serializer
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl<'a, K, V, P> Default for ProducerBuilder<'a, K, V, P>
    where K: Serializer,
          V: Serializer
{
    fn default() -> Self {
        ProducerBuilder {
            config: ProducerConfig::default(),
            client: None,
            handle: None,
            key_serializer: None,
            value_serializer: None,
            partitioner: None,
            interceptors: None,
        }
    }
}

impl<'a, K, V, P> ProducerBuilder<'a, K, V, P>
    where K: Serializer,
          V: Serializer
{
    /// Construct a `ProducerBuilder` from KafkaClient
    pub fn from_client(client: KafkaClient<'a>) -> Self {
        ProducerBuilder {
            client: Some(client),
            handle: None,
            config: ProducerConfig::default(),
            key_serializer: None,
            value_serializer: None,
            partitioner: None,
            interceptors: None,
        }
    }

    /// Construct a `ProducerBuilder` from ProducerConfig
    pub fn from_config(config: ProducerConfig, handle: Handle) -> Self {
        ProducerBuilder {
            client: None,
            handle: Some(handle),
            config: config,
            key_serializer: None,
            value_serializer: None,
            partitioner: None,
            interceptors: None,
        }
    }

    /// Construct a `ProducerBuilder` from brokers
    pub fn from_hosts<I>(hosts: I, handle: Handle) -> Self
        where I: Iterator<Item = SocketAddr>
    {
        Self::from_config(ProducerConfig::from_hosts(hosts), handle)
    }
}

impl<'a, K, V, P> ProducerBuilder<'a, K, V, P>
    where K: Serializer,
          V: Serializer
{
    fn with_client(mut self, client: KafkaClient<'a>) -> Self {
        self.client = Some(client);
        self
    }

    fn with_handle(mut self, handle: Handle) -> Self {
        self.handle = Some(handle);
        self
    }

    /// Sets the id string to pass to the server when making requests.
    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.config.client_id = Some(client_id);
        self
    }

    /// Sets the number of milliseconds after this we close idle connections
    pub fn with_max_connection_idle(mut self, max_connection_idle: Duration) -> Self {
        self.config.max_connection_idle = max_connection_idle.as_millis();
        self
    }

    /// Sets the maximum amount of time the client will wait for the response of a request.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.config.request_timeout = request_timeout.as_millis();
        self
    }

    /// Sets the request broker's supported API versions to adjust functionality to available protocol features.
    pub fn with_api_version_request(mut self) -> Self {
        self.config.api_version_request = true;
        self
    }

    /// Sets the fallback broker version will be used
    pub fn with_broker_version_fallback(mut self, version: KafkaVersion) -> Self {
        self.config.broker_version_fallback = version;
        self
    }

    /// Sets the period of time in milliseconds after which we force a refresh of metadata
    pub fn with_metadata_max_age(mut self, metadata_max_age: Duration) -> Self {
        self.config.metadata_max_age = metadata_max_age.as_millis();
        self
    }

    /// Sets to record metrics for client operations
    pub fn with_metrics(mut self) -> Self {
        self.config.metrics = true;
        self
    }

    /// Sets the number of acknowledgments the producer requires the leader
    /// to have received before considering a request complete.
    pub fn with_required_acks(mut self, acks: RequiredAcks) -> Self {
        self.config.acks = acks;
        self
    }

    /// Sets the compression type for all data generated by the producer.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    /// Sets the size in bytes that the producer will attempt to batch records together
    /// into fewer requests whenever multiple records are being sent to the same partition.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.batch_size = batch_size;
        self
    }

    /// Sets the maximum amount of time the server will wait for acknowledgments
    /// from followers to meet the acknowledgment requirements
    pub fn with_ack_timeout(mut self, ack_timeout: Duration) -> Self {
        self.config.ack_timeout = ack_timeout.as_millis();
        self
    }

    /// Sets the time that the producer groups together any records
    /// that arrive in between request transmissions into a single batched request.
    pub fn with_linger(mut self, linger: Duration) -> Self {
        self.config.linger = linger.as_millis();
        self
    }

    /// Sets the key serializer that serialize key to record
    pub fn with_key_serializer(mut self, key_serializer: K) -> Self {
        self.key_serializer = Some(key_serializer);
        self
    }

    /// Sets the value serializer that serialize value to record
    pub fn with_value_serializer(mut self, value_serializer: V) -> Self {
        self.value_serializer = Some(value_serializer);
        self
    }

    /// Sets the partitioner which choosing a partition for a message to be sent to Kafka.
    pub fn with_partitioner(mut self, partitioner: P) -> Self {
        self.partitioner = Some(partitioner);
        self
    }

    /// Sets the interceptor which intercepte (and possibly mutate) the records
    /// received by the producer before they are published to the Kafka cluster.
    pub fn with_interceptor<I>(mut self, interceptor: I) -> Self
        where I: ProducerInterceptor<Key = K::Item, Value = V::Item> + 'static,
              K::Item: Hash
    {
        let interceptors =
            self.interceptors
                .unwrap_or_else(|| Rc::new(RefCell::new(ProducerInterceptors::new())));

        interceptors.borrow_mut().push(Box::new(interceptor));

        self.interceptors = Some(interceptors);
        self
    }
}

impl<'a, V, P> ProducerBuilder<'a, NoopSerializer<()>, V, P>
    where V: Serializer
{
    /// Sets the key serializer to empty
    pub fn without_key_serializer(mut self) -> Self {
        self.key_serializer = Some(NoopSerializer::default());
        self
    }
}

impl<'a, K, P> ProducerBuilder<'a, K, NoopSerializer<()>, P>
    where K: Serializer
{
    /// Sets the value serializer to empty
    pub fn without_value_serializer(mut self) -> Self {
        self.value_serializer = Some(NoopSerializer::default());
        self
    }
}

impl<'a, K, V> ProducerBuilder<'a, K, V, DefaultPartitioner>
    where K: Serializer,
          V: Serializer
{
    /// Sets the default partitioner
    pub fn with_default_partitioner(mut self) -> Self {
        self.partitioner = Some(DefaultPartitioner::default());
        self
    }
}

impl<'a, K, V, P> ProducerBuilder<'a, K, V, P>
    where K: Serializer,
          K::Item: Hash,
          V: Serializer,
          Self: 'static
{
    /// Construct a `KafkaProducer`
    pub fn build(self) -> Result<KafkaProducer<'a, K, V, P>> {
        let client = if let Some(client) = self.client {
            client
        } else {
            KafkaClient::from_config(self.config.client.clone(),
                                     self.handle.ok_or(ErrorKind::ConfigError("missed handle"))?)
        };

        Ok(KafkaProducer::new(client,
                              self.config,
                              self.key_serializer
                                  .ok_or(ErrorKind::ConfigError("missed key serializer"))?,
                              self.value_serializer
                                  .ok_or(ErrorKind::ConfigError("missed value serializer"))?,
                              self.partitioner
                                  .ok_or(ErrorKind::ConfigError("missed partitioner"))?,
                              self.interceptors))
    }
}

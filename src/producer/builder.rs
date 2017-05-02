use std::ops::{Deref, DerefMut};
use std::net::ToSocketAddrs;

use tokio_core::reactor::Handle;

use errors::{ErrorKind, Result};
use client::KafkaClient;
use producer::{DefaultPartitioner, KafkaProducer, Partitioner, ProducerConfig};

pub struct ProducerBuilder<'a, A, K, V, P = DefaultPartitioner>
    where A: ToSocketAddrs,
          P: Partitioner
{
    client: Option<KafkaClient<'a>>,
    handle: Option<Handle>,
    config: ProducerConfig<A>,
    partitioner: P,
    key_serializer: Option<K>,
    value_serializer: Option<V>,
}

impl<'a, A, K, V, P> Deref for ProducerBuilder<'a, A, K, V, P>
    where A: ToSocketAddrs,
          P: Partitioner
{
    type Target = ProducerConfig<A>;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl<'a, A, K, V, P> DerefMut for ProducerBuilder<'a, A, K, V, P>
    where A: ToSocketAddrs,
          P: Partitioner
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl<'a, A, K, V> ProducerBuilder<'a, A, K, V, DefaultPartitioner>
    where A: ToSocketAddrs
{
    pub fn from_client(client: KafkaClient<'a>,
                       config: ProducerConfig<A>)
                       -> ProducerBuilder<'a, A, K, V, DefaultPartitioner> {
        ProducerBuilder {
            client: Some(client),
            handle: None,
            config: config,
            partitioner: DefaultPartitioner::default(),
            key_serializer: None,
            value_serializer: None,
        }
    }

    pub fn from_config(config: ProducerConfig<A>,
                       handle: Handle)
                       -> ProducerBuilder<'a, A, K, V, DefaultPartitioner> {
        ProducerBuilder {
            client: None,
            handle: Some(handle),
            config: config,
            partitioner: DefaultPartitioner::default(),
            key_serializer: None,
            value_serializer: None,
        }
    }
}

impl<'a, A, K, V, P> ProducerBuilder<'a, A, K, V, P>
    where A: ToSocketAddrs + Clone,
          P: Partitioner,
          Self: 'static
{
    pub fn build(self) -> Result<KafkaProducer<'a, A, K, V>> {
        let client = if let Some(client) = self.client {
            client
        } else if let Some(handle) = self.handle {
            KafkaClient::from_hosts(&self.config.hosts[..], handle)
        } else {
            bail!(ErrorKind::ConfigError("missed client or handle"))
        };

        Ok(KafkaProducer::from_client(client, self.config))
    }
}
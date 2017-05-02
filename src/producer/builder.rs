use std::ops::{Deref, DerefMut};
use std::net::ToSocketAddrs;

use tokio_core::reactor::Handle;

use errors::{ErrorKind, Result};
use client::KafkaClient;
use producer::{DefaultPartitioner, KafkaProducer, Partitioner, ProducerConfig};

pub struct ProducerBuilder<A, P = DefaultPartitioner>
    where A: ToSocketAddrs,
          P: Partitioner
{
    client: Option<KafkaClient>,
    handle: Option<Handle>,
    config: ProducerConfig<A>,
    partitioner: P,
}

impl<A, P> Deref for ProducerBuilder<A, P>
    where A: ToSocketAddrs,
          P: Partitioner
{
    type Target = ProducerConfig<A>;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl<A, P> DerefMut for ProducerBuilder<A, P>
    where A: ToSocketAddrs,
          P: Partitioner
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl<A> ProducerBuilder<A, DefaultPartitioner>
    where A: ToSocketAddrs
{
    pub fn from_client(client: KafkaClient,
                       config: ProducerConfig<A>)
                       -> ProducerBuilder<A, DefaultPartitioner> {
        ProducerBuilder {
            client: Some(client),
            handle: None,
            config: config,
            partitioner: DefaultPartitioner::default(),
        }
    }

    pub fn from_config(config: ProducerConfig<A>,
                       handle: Handle)
                       -> ProducerBuilder<A, DefaultPartitioner> {
        ProducerBuilder {
            client: None,
            handle: Some(handle),
            config: config,
            partitioner: DefaultPartitioner::default(),
        }
    }
}

impl<A, P> ProducerBuilder<A, P>
    where A: ToSocketAddrs + Clone,
          P: Partitioner
{
    pub fn build(self) -> Result<KafkaProducer<A>> {
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
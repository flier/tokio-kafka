use std::net::ToSocketAddrs;

use tokio_core::reactor::Handle;

use client::{KafkaConfig, KafkaClient};

pub struct KafkaProducer {
    client: KafkaClient,
}

impl KafkaProducer {
    pub fn from_client(client: KafkaClient) -> Self {
        KafkaProducer { client: client }
    }

    pub fn from_config(config: KafkaConfig, handle: &Handle) -> Self {
        KafkaProducer::from_client(KafkaClient::from_config(config, handle))
    }

    pub fn from_hosts<A: ToSocketAddrs + Clone>(hosts: &[A], handle: &Handle) -> Self {
        KafkaProducer::from_config(KafkaConfig::from_hosts(hosts), handle)
    }

    pub fn client(&self) -> &KafkaClient {
        &self.client
    }

    pub fn into_client(self) -> KafkaClient {
        self.client
    }
}

impl From<KafkaClient> for KafkaProducer {
    fn from(client: KafkaClient) -> Self {
        KafkaProducer::from_client(client)
    }
}

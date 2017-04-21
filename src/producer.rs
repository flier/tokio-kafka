use std::net::ToSocketAddrs;

use client::{KafkaConfig, KafkaClient};

pub struct KafkaProducer {
    client: KafkaClient,
}

impl KafkaProducer {
    pub fn from_client(client: KafkaClient) -> Self {
        KafkaProducer { client: client }
    }

    pub fn from_config(config: KafkaConfig) -> Self {
        KafkaProducer::from_client(KafkaClient::from_config(config))
    }

    pub fn from_hosts<A: ToSocketAddrs + Clone>(hosts: &[A]) -> Self {
        KafkaProducer::from_config(KafkaConfig::from_hosts(hosts))
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

impl From<KafkaConfig> for KafkaProducer {
    fn from(config: KafkaConfig) -> Self {
        KafkaProducer::from_config(config)
    }
}

impl<'a, A: ToSocketAddrs + Clone> From<&'a [A]> for KafkaProducer {
    fn from(hosts: &'a [A]) -> Self {
        KafkaProducer::from_hosts(hosts)
    }
}
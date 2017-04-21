use std::ops::{Deref, DerefMut};
use std::net::ToSocketAddrs;

use client::{KafkaConfig, KafkaState};

pub struct KafkaClient {
    config: KafkaConfig,
    state: KafkaState,
}

impl Deref for KafkaClient {
    type Target = KafkaConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl DerefMut for KafkaClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl KafkaClient {
    pub fn from_config(config: KafkaConfig) -> Self {
        KafkaClient {
            config: config,
            state: KafkaState::new(),
        }
    }

    pub fn from_hosts<A: ToSocketAddrs + Clone>(hosts: &[A]) -> Self {
        KafkaClient::from_config(KafkaConfig::from_hosts(hosts))
    }
}
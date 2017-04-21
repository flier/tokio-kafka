use std::time::Duration;
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::{Deref, DerefMut};

use tokio_core::reactor::Handle;

use errors::{Error, ErrorKind, Result};
use network::KafkaConnectionPool;
use client::{KafkaOption, KafkaConfig, KafkaState, DEFAULT_MAX_CONNECTION_TIMEOUT,
             DEFAULT_MAX_POOLED_CONNECTIONS};

pub struct KafkaClient {
    config: KafkaConfig,
    conns: KafkaConnectionPool,
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
        let max_connection_idle = config
            .max_connection_idle()
            .unwrap_or(*DEFAULT_MAX_CONNECTION_TIMEOUT);
        let max_pooled_connections = config
            .max_pooled_connections()
            .unwrap_or(DEFAULT_MAX_POOLED_CONNECTIONS);

        KafkaClient {
            config: config,
            conns: KafkaConnectionPool::new(max_connection_idle, max_pooled_connections),
            state: KafkaState::new(),
        }
    }

    pub fn from_hosts<A: ToSocketAddrs + Clone>(hosts: &[A]) -> Self {
        KafkaClient::from_config(KafkaConfig::from_hosts(hosts))
    }

    fn fetch_metadata<T: AsRef<str>>(&mut self, topic: &[T], handle: &Handle) -> Result<()> {
        for addr in self.config.brokers().ok_or(ErrorKind::NoHostError)? {
            let conn = self.conns.get(&addr, handle);
        }

        Ok(())
    }
}
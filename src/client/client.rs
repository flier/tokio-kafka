use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::{Deref, DerefMut};

use futures::future::{self, Future, BoxFuture, select_ok};
use tokio_core::reactor::Handle;
use tokio_proto::BindClient;
use tokio_timer::Timer;

use errors::{Error, ErrorKind, Result};
use network::KafkaConnectionPool;
use client::{KafkaOption, KafkaConfig, KafkaState, KafkaProto, DEFAULT_MAX_CONNECTION_TIMEOUT,
             DEFAULT_MAX_POOLED_CONNECTIONS};

pub struct KafkaClient {
    config: KafkaConfig,
    conns: Arc<KafkaConnectionPool>,
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
            conns: Arc::new(KafkaConnectionPool::new(max_connection_idle, max_pooled_connections)),
            state: KafkaState::new(),
        }
    }

    pub fn from_hosts<A: ToSocketAddrs + Clone>(hosts: &[A]) -> Self {
        KafkaClient::from_config(KafkaConfig::from_hosts(hosts))
    }

    pub fn load_metadata(&mut self, handle: &Handle) -> BoxFuture<(), Error> {
        self.fetch_metadata::<&str>(&[], handle)
    }

    fn fetch_metadata<T>(&mut self, topics: &[T], handle: &Handle) -> BoxFuture<(), Error>
        where T: AsRef<str>
    {
        let handle = Arc::new(Mutex::new(handle.clone()));

        select_ok(self.config
                      .brokers()
                      .unwrap()
                      .iter()
                      .map(|addr| self.conns.get(&addr, &handle.clone().lock().unwrap())))
                .and_then(move |(conn, rest)| {
                    for conn in rest {
                        conn.map(|conn| self.conns.release(conn));
                    }

                    future::ok(KafkaProto::new(0).bind_client(&handle.clone().lock().unwrap(),
                                                              conn))
                })
                .and_then(|service| future::ok(()))
                .boxed()
    }
}
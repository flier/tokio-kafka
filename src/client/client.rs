use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::iter::FromIterator;
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::{Deref, DerefMut};

use futures::{Async, Poll};
use futures::future::{self, Future, BoxFuture, select_ok};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_core::reactor::Handle;
use tokio_proto::BindClient;
use tokio_proto::pipeline::ClientService;
use tokio_service::{Service, NewService};
use tokio_timer::Timer;

use errors::{Error, ErrorKind, Result};
use network::{KafkaConnection, KafkaConnectionPool};
use protocol::{ApiVersion, MetadataRequest, MetadataResponse};
use client::{KafkaOption, KafkaConfig, KafkaState, KafkaProto, KafkaRequest, KafkaResponse,
             Metadata, DEFAULT_MAX_CONNECTION_TIMEOUT, DEFAULT_MAX_POOLED_CONNECTIONS};

pub struct KafkaClient {
    config: KafkaConfig,
    handle: Handle,
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
    pub fn from_config(config: KafkaConfig, handle: &Handle) -> Self {
        let max_connection_idle = config
            .max_connection_idle()
            .unwrap_or(*DEFAULT_MAX_CONNECTION_TIMEOUT);
        let max_pooled_connections = config
            .max_pooled_connections()
            .unwrap_or(DEFAULT_MAX_POOLED_CONNECTIONS);

        KafkaClient {
            config: config,
            handle: handle.clone(),
            conns: KafkaConnectionPool::new(max_connection_idle, max_pooled_connections),
            state: KafkaState::new(),
        }
    }

    pub fn from_hosts<A: ToSocketAddrs + Clone>(hosts: &[A], handle: &Handle) -> Self {
        KafkaClient::from_config(KafkaConfig::from_hosts(hosts), handle)
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    pub fn service<T>(&self, api_version: ApiVersion, io: T) -> ClientService<T, KafkaProto>
        where T: 'static + AsyncRead + AsyncWrite
    {
        KafkaProto::new(api_version).bind_client(&self.handle, io)
    }

    pub fn load_metadata(&mut self, handle: &Handle) -> BoxFuture<(), Error> {
        self.fetch_metadata::<&str>(&[], handle)
            .and_then(|metadata| {
                          //self.state.update_metadata(metadata);

                          future::ok(())
                      })
            .boxed()
    }

    fn fetch_metadata<S>(&mut self,
                         topic_names: &[S],
                         handle: &Handle)
                         -> BoxFuture<Metadata, Error>
        where S: AsRef<str>
    {
        let conns = self.config
            .brokers()
            .unwrap()
            .iter()
            .map(|addr| self.conns.checkout(&addr, handle));

        let api_version = ApiVersion::Kafka_0_8;
        let correlation_id = self.state.next_correlation_id();
        let client_id = self.config.client_id();
        let request = MetadataRequest::new(api_version, correlation_id, client_id, topic_names);
        let handle = self.handle.clone();
        let create_service = move |conn| KafkaProto::new(api_version).bind_client(&handle, conn);

        select_ok(conns)
            .map_err(Error::from)
            .and_then(|(conn, rest)| {
                for conn in rest {
                    conn.map(|conn| self.conns.release(conn));
                }

                let service = create_service(conn);

                service
                    .call(KafkaRequest::Metadata(request))
                    .map_err(Error::from)
            })
            .map_err(Error::from)
            .and_then(|res| if let KafkaResponse::Metadata(res) = res {
                          future::ok(Metadata::from(res))
                      } else {
                          future::err(ErrorKind::OtherError.into())
                      })
            .boxed()
    }
}

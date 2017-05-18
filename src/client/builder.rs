use std::time::Duration;
use std::ops::{Deref, DerefMut};
use std::net::SocketAddr;
use std::marker::PhantomData;

use tokio_core::reactor::Handle;

use errors::{ErrorKind, Result};
use protocol::ToMilliseconds;
use client::{ClientConfig, KafkaClient, KafkaVersion};

#[derive(Default)]
pub struct ClientBuilder<'a> {
    config: ClientConfig,
    handle: Option<Handle>,
    phantom: PhantomData<&'a u8>,
}

impl<'a> Deref for ClientBuilder<'a> {
    type Target = ClientConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl<'a> DerefMut for ClientBuilder<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

pub fn from_hosts<'a, I>(hosts: I, handle: Handle) -> ClientBuilder<'a>
    where I: Iterator<Item = SocketAddr>
{
    ClientBuilder::from_hosts(hosts, handle)
}

impl<'a> ClientBuilder<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn from_config(config: ClientConfig, handle: Handle) -> Self {
        ClientBuilder {
            config: config,
            handle: Some(handle),
            phantom: PhantomData,
        }
    }

    pub fn from_hosts<I>(hosts: I, handle: Handle) -> Self
        where I: Iterator<Item = SocketAddr>
    {
        Self::from_config(ClientConfig::from_hosts(hosts), handle)
    }

    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.config.client_id = Some(client_id);
        self
    }

    pub fn with_max_connection_idle(mut self, max_connection_idle: Duration) -> Self {
        self.config.max_connection_idle = max_connection_idle.as_millis();
        self
    }

    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.config.request_timeout = request_timeout.as_millis();
        self
    }

    pub fn with_api_version_request(mut self) -> Self {
        self.config.api_version_request = true;
        self
    }

    pub fn with_broker_version_fallback(mut self, version: KafkaVersion) -> Self {
        self.config.broker_version_fallback = version;
        self
    }

    pub fn with_metadata_max_age(mut self, metadata_max_age: Duration) -> Self {
        self.config.metadata_max_age = metadata_max_age.as_millis();
        self
    }

    pub fn with_metrics(mut self) -> Self {
        self.config.metrics = true;
        self
    }

    pub fn with_handle(mut self, handle: Handle) -> Self {
        self.handle = Some(handle);
        self
    }
}

impl<'a> ClientBuilder<'a>
    where Self: 'static
{
    pub fn build(self) -> Result<KafkaClient<'a>> {
        let handle = self.handle
            .ok_or(ErrorKind::ConfigError("missed handle"))?;

        Ok(KafkaClient::from_config(self.config, handle))
    }
}

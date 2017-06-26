use std::marker::PhantomData;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use tokio_core::reactor::Handle;

use client::{ClientConfig, KafkaClient, KafkaVersion};
use errors::{ErrorKind, Result};
use protocol::ToMilliseconds;

/// A `KafkaClient` builder easing the process of setting up various configuration settings.
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

impl<'a> ClientBuilder<'a> {
    /// Construct a `ClientBuilder` from ClientConfig
    pub fn with_config(config: ClientConfig, handle: Handle) -> Self {
        ClientBuilder {
            config: config,
            handle: Some(handle),
            phantom: PhantomData,
        }
    }

    /// Construct a `ClientBuilder` from brokers
    pub fn with_bootstrap_servers<I>(hosts: I, handle: Handle) -> Self
    where
        I: IntoIterator<Item = SocketAddr>,
    {
        Self::with_config(ClientConfig::with_bootstrap_servers(hosts), handle)
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

    /// Sets the request broker's supported API versions to adjust functionality to available
    /// protocol features.
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
}

impl<'a> ClientBuilder<'a>
where
    Self: 'static,
{
    pub fn build(self) -> Result<KafkaClient<'a>> {
        let handle = self.handle.ok_or(ErrorKind::ConfigError("missed handle"))?;

        Ok(KafkaClient::new(self.config, handle))
    }
}

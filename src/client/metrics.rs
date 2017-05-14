use std::ops::{Deref, DerefMut};

use prometheus::{CounterVec, Registry};

use errors::Result;
use protocol::ApiKeys;
use network::{KafkaRequest, KafkaResponse};

pub const NAMESPACE_KAFKA: &str = "kafka";
pub const SUBSYSTEM_CLIENT: &str = "client";

pub struct Metrics {
    registry: Registry,

    send_requests: CounterVec,
    received_responses: CounterVec,
}

impl Deref for Metrics {
    type Target = Registry;

    fn deref(&self) -> &Self::Target {
        &self.registry
    }
}

impl DerefMut for Metrics {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.registry
    }
}

impl Metrics {
    pub fn new() -> Result<Self> {
        let registry = Registry::new();

        let send_requests = CounterVec::new(opts!("api_requests", "API requests")
                                                .namespace(NAMESPACE_KAFKA.to_owned())
                                                .subsystem(SUBSYSTEM_CLIENT.to_owned()),
                                            &["api_key", "api_version"])?;
        let received_responses = CounterVec::new(opts!("received_responses", "API responses")
                                                     .namespace(NAMESPACE_KAFKA.to_owned())
                                                     .subsystem(SUBSYSTEM_CLIENT.to_owned()),
                                                 &["api_key"])?;

        registry.register(Box::new(send_requests.clone()))?;
        registry.register(Box::new(received_responses.clone()))?;

        Ok(Metrics {
               registry: registry,
               send_requests: send_requests,
               received_responses: received_responses,
           })
    }

    pub fn send_request(&self, request: &KafkaRequest) {
        let header = request.header();

        self.send_requests
            .with_label_values(&[ApiKeys::from(header.api_key).name(),
                                 &header.api_version.to_string()])
            .inc()
    }

    pub fn received_response(&self, response: &KafkaResponse) {
        self.received_responses
            .with_label_values(&[response.api_key().name()])
            .inc()
    }
}

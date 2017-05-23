use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

use prometheus::{CounterVec, GaugeVec, Registry};

use errors::Result;
use protocol::ApiKeys;
use network::{KafkaRequest, KafkaResponse};

pub const NAMESPACE_KAFKA: &str = "kafka";
pub const SUBSYSTEM_CLIENT: &str = "client";

pub struct Metrics {
    registry: Registry,

    send_requests: CounterVec,
    in_flight_requests: GaugeVec,
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

        let send_requests = CounterVec::new(opts!("api_requests", "sent API requests")
                                                .namespace(NAMESPACE_KAFKA.to_owned())
                                                .subsystem(SUBSYSTEM_CLIENT.to_owned()),
                                            &["broker", "api_key", "api_version"])?;

        let in_flight_requests =
            GaugeVec::new(opts!("in_flight_requests", "In flight API requests")
                              .namespace(NAMESPACE_KAFKA.to_owned())
                              .subsystem(SUBSYSTEM_CLIENT.to_owned()),
                          &["broker", "api_key"])?;

        let received_responses =
            CounterVec::new(opts!("received_responses", "recieved API responses")
                                .namespace(NAMESPACE_KAFKA.to_owned())
                                .subsystem(SUBSYSTEM_CLIENT.to_owned()),
                            &["broker", "api_key"])?;

        registry.register(Box::new(send_requests.clone()))?;
        registry.register(Box::new(in_flight_requests.clone()))?;
        registry.register(Box::new(received_responses.clone()))?;

        Ok(Metrics {
               registry: registry,
               send_requests: send_requests,
               in_flight_requests: in_flight_requests,
               received_responses: received_responses,
           })
    }

    pub fn send_request(&self, addr: &SocketAddr, request: &KafkaRequest) {
        let header = request.header();
        let labels = [&addr.to_string(),
                      ApiKeys::from(header.api_key).name(),
                      &header.api_version.to_string()];

        self.send_requests.with_label_values(&labels).inc();
        self.in_flight_requests
            .with_label_values(&labels[..2])
            .inc();
    }

    pub fn received_response(&self, addr: &SocketAddr, response: &KafkaResponse) {
        let labels = [&addr.to_string(), response.api_key().name()];

        self.received_responses.with_label_values(&labels).inc();
        self.in_flight_requests.with_label_values(&labels).dec();
    }
}

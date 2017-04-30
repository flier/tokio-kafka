use std::rc::Rc;

use protocol::CorrelationId;
use network::ConnectionId;
use client::Metadata;

pub struct KafkaState {
    connection_id: ConnectionId,

    correlation_id: CorrelationId,

    metadata: Rc<Metadata>,
}

impl KafkaState {
    pub fn new() -> Self {
        KafkaState {
            connection_id: 0,
            correlation_id: 0,
            metadata: Rc::new(Metadata::default()),
        }
    }

    pub fn next_connection_id(&mut self) -> ConnectionId {
        self.connection_id = self.connection_id.wrapping_add(1);
        self.connection_id - 1
    }

    pub fn next_correlation_id(&mut self) -> CorrelationId {
        self.correlation_id = self.correlation_id.wrapping_add(1);
        self.correlation_id - 1
    }

    pub fn metadata(&self) -> Rc<Metadata> {
        self.metadata.clone()
    }

    pub fn update_metadata(&mut self, metadata: Metadata) {
        debug!("updating metadata, {:?}", metadata);

        self.metadata = Rc::new(metadata);
    }
}
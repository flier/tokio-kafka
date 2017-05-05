use std::rc::Rc;
use std::marker::PhantomData;

use protocol::CorrelationId;
use network::ConnectionId;
use client::Metadata;

pub struct KafkaState<'a> {
    connection_id: ConnectionId,
    correlation_id: CorrelationId,
    metadata: Rc<Metadata>,
    phantom: PhantomData<&'a u8>,
}

impl<'a> KafkaState<'a> {
    pub fn new() -> Self {
        KafkaState {
            connection_id: 0,
            correlation_id: 0,
            metadata: Rc::new(Metadata::default()),
            phantom: PhantomData,
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

    pub fn update_metadata(&mut self, metadata: Rc<Metadata>) -> Rc<Metadata> {
        debug!("updating metadata, {:?}", metadata);

        self.metadata = metadata;
        self.metadata.clone()
    }
}

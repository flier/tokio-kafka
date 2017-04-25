use std::rc::Rc;

use client::Metadata;

pub struct KafkaState {
    // ~ the last correlation used when communicating with kafka
    // (see `#next_correlation_id`)
    correlation: i32,

    metadata: Rc<Metadata>,
}

impl KafkaState {
    pub fn new() -> Self {
        KafkaState {
            correlation: 0,
            metadata: Rc::new(Metadata::default()),
        }
    }

    pub fn next_correlation_id(&mut self) -> i32 {
        self.correlation = self.correlation.wrapping_add(1);
        self.correlation
    }

    pub fn metadata(&self) -> Rc<Metadata> {
        self.metadata.clone()
    }

    pub fn update_metadata(&mut self, metadata: Metadata) {
        debug!("updating metadata, {:?}", metadata);

        self.metadata = Rc::new(metadata);
    }
}
use std::collections::hash_map::HashMap;

use client::{Metadata, Broker, BrokerRef, TopicPartitions};

pub struct KafkaState {
    // ~ the last correlation used when communicating with kafka
    // (see `#next_correlation_id`)
    correlation: i32,

    // ~ a list of known brokers referred to by the index in this
    // vector.  This index is also referred to as `BrokerRef` and is
    // enforced by this module.
    //
    // Note: loading of additional topic metadata must preserve
    // already present brokers in this vector at their position.
    // See `KafkaState::update_metadata`
    brokers: Vec<Broker>,

    // ~ a mapping of topic to information about its partitions
    topic_partitions: HashMap<String, TopicPartitions>,

    // ~ a mapping of groups to their coordinators
    group_coordinators: HashMap<String, BrokerRef>,
}

impl KafkaState {
    pub fn new() -> Self {
        KafkaState {
            correlation: 0,
            brokers: Vec::new(),
            topic_partitions: HashMap::new(),
            group_coordinators: HashMap::new(),
        }
    }

    pub fn next_correlation_id(&mut self) -> i32 {
        self.correlation = self.correlation.wrapping_add(1);
        self.correlation
    }

    pub fn update_metadata(&mut self, md: Metadata) {
        self.brokers = md.brokers;
        self.topic_partitions = md.topics;
    }
}
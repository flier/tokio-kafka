use std::net::ToSocketAddrs;

use client::KafkaClient;

use producer::{DefaultPartitioner, KafkaProducer, Partitioner, ProducerConfig};

pub struct ProducerBuilder<A: ToSocketAddrs, P: Partitioner = DefaultPartitioner<KafkaProducer>> {
    client: Option<KafkaClient>,
    config: ProducerConfig<A>,
    partitioner: P,
}
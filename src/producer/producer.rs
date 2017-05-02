use std::net::ToSocketAddrs;

use tokio_core::reactor::Handle;

use client::KafkaClient;
use producer::{DefaultPartitioner, ProducerBuilder, ProducerConfig};

pub struct KafkaProducer<A>
    where A: ToSocketAddrs
{
    client: KafkaClient,
    config: ProducerConfig<A>,
}

impl<A> KafkaProducer<A>
    where A: ToSocketAddrs
{
    pub fn from_client(client: KafkaClient, config: ProducerConfig<A>) -> Self {
        KafkaProducer {
            client: client,
            config: config,
        }
    }

    pub fn client(&self) -> &KafkaClient {
        &self.client
    }

    pub fn into_client(self) -> KafkaClient {
        self.client
    }
}

impl<A> KafkaProducer<A>
    where A: ToSocketAddrs
{
    pub fn from_hosts(hosts: &[A], handle: Handle) -> ProducerBuilder<A, DefaultPartitioner>
        where A: ToSocketAddrs + Clone
    {
        ProducerBuilder::from_config(ProducerConfig::from_hosts(hosts), handle)
    }
}

#[cfg(test)]
pub mod mock {
    use std::mem;
    use std::hash::Hash;
    use std::collections::HashMap;

    use futures::future;

    use client::TopicPartition;
    use producer::{FlushProducer, Producer, ProducerRecord, RecordMetadata, SendRecord};

    #[derive(Debug, Default)]
    pub struct MockProducer<'a, K, V>
        where K: Hash
    {
        pub topics: HashMap<String, Vec<TopicPartition<'a>>>,
        pub records: Vec<(Option<K>, V)>,
    }

    impl<'a, K, V> Producer for MockProducer<'a, K, V>
        where K: Hash + Clone,
              V: Clone
    {
        type Key = K;
        type Value = V;

        fn partitions_for(&self, topic: &str) -> Option<&[TopicPartition]> {
            self.topics
                .get(topic)
                .map(|partitions| partitions.as_slice())
        }

        fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord {
            self.records.push((record.key, record.value));

            SendRecord::new(future::ok(RecordMetadata { ..unsafe { mem::zeroed() } }))
        }

        fn flush(&mut self) -> FlushProducer {
            FlushProducer::new(future::ok(()))
        }
    }
}
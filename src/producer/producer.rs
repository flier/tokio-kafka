use std::mem;
use std::hash::Hash;
use std::net::SocketAddr;

use futures::future;
use tokio_core::reactor::Handle;

use client::{Cluster, KafkaClient, TopicPartition};
use producer::{DefaultPartitioner, FlushProducer, Producer, ProducerBuilder, ProducerConfig,
               ProducerRecord, RecordMetadata, SendRecord, Serializer};

pub struct KafkaProducer<'a, K, V> {
    client: KafkaClient<'a>,
    config: ProducerConfig,
    key_serializer: Option<K>,
    value_serializer: Option<V>,
}

impl<'a, K, V> KafkaProducer<'a, K, V> {
    pub fn from_client(client: KafkaClient<'a>, config: ProducerConfig) -> KafkaProducer<'a, K, V> {
        KafkaProducer {
            client: client,
            config: config,
            key_serializer: None,
            value_serializer: None,
        }
    }

    pub fn client(&self) -> &KafkaClient<'a> {
        &self.client
    }

    pub fn into_client(self) -> KafkaClient<'a> {
        self.client
    }
}

impl<'a, K, V> KafkaProducer<'a, K, V> {
    pub fn from_hosts<I>(hosts: I, handle: Handle) -> ProducerBuilder<'a, K, V, DefaultPartitioner>
        where I: Iterator<Item = SocketAddr>
    {
        ProducerBuilder::from_config(ProducerConfig::from_hosts(hosts), handle)
    }
}

impl<'a, K, V> Producer<'a> for KafkaProducer<'a, K, V>
    where K: Serializer,
          K::Item: Hash,
          V: Serializer,
          Self: 'static
{
    type Key = K::Item;
    type Value = V::Item;

    fn partitions_for(&'a self, toipc_name: &'a str) -> Option<Vec<TopicPartition<'a>>> {
        self.client.metadata().partitions_for_topic(toipc_name)
    }

    fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord {
        SendRecord::new(future::ok(RecordMetadata { ..unsafe { mem::zeroed() } }))
    }

    fn flush(&mut self) -> FlushProducer {
        FlushProducer::new(future::ok(()))
    }
}

#[cfg(test)]
pub mod mock {
    use std::mem;
    use std::hash::Hash;
    use std::collections::HashMap;

    use futures::future;

    use client::TopicPartition;
    use protocol::PartitionId;
    use producer::{FlushProducer, Producer, ProducerRecord, RecordMetadata, SendRecord};

    #[derive(Debug, Default)]
    pub struct MockProducer<K, V>
        where K: Hash
    {
        pub topics: HashMap<String, Vec<(String, PartitionId)>>,
        pub records: Vec<(Option<K>, V)>,
    }

    impl<'a, K, V> Producer<'a> for MockProducer<K, V>
        where K: Hash + Clone,
              V: Clone
    {
        type Key = K;
        type Value = V;

        fn partitions_for(&self, topic_name: &'a str) -> Option<Vec<TopicPartition<'a>>> {
            self.topics
                .get(topic_name)
                .map(|partitions| {
                    partitions
                        .iter()
                        .map(|&(_, partition)| {
                                 TopicPartition {
                                     topic_name: topic_name,
                                     partition: partition,
                                 }
                             })
                        .collect()
                })
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
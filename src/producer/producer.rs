use std::mem;
use std::hash::Hash;
use std::net::ToSocketAddrs;

use futures::future;
use tokio_core::reactor::Handle;

use client::{Cluster, KafkaClient, TopicPartition};
use producer::{DefaultPartitioner, FlushProducer, Producer, ProducerBuilder, ProducerConfig,
               ProducerRecord, RecordMetadata, SendRecord, Serializer};

pub struct KafkaProducer<A, K, V>
    where A: ToSocketAddrs
{
    client: KafkaClient,
    config: ProducerConfig<A>,
    key_serializer: Option<K>,
    value_serializer: Option<V>,
}

impl<A, K, V> KafkaProducer<A, K, V>
    where A: ToSocketAddrs
{
    pub fn from_client(client: KafkaClient, config: ProducerConfig<A>) -> Self {
        KafkaProducer {
            client: client,
            config: config,
            key_serializer: None,
            value_serializer: None,
        }
    }

    pub fn client(&self) -> &KafkaClient {
        &self.client
    }

    pub fn into_client(self) -> KafkaClient {
        self.client
    }
}

impl<A, K, V> KafkaProducer<A, K, V>
    where A: ToSocketAddrs
{
    pub fn from_hosts(hosts: &[A], handle: Handle) -> ProducerBuilder<A, K, V, DefaultPartitioner>
        where A: ToSocketAddrs + Clone
    {
        ProducerBuilder::from_config(ProducerConfig::from_hosts(hosts), handle)
    }
}

impl<A, K, V> Producer for KafkaProducer<A, K, V>
    where A: ToSocketAddrs,
          K: Serializer,
          K::Item: Hash,
          V: Serializer
{
    type Key = K::Item;
    type Value = V::Item;

    fn partitions_for<'a>(&self, toipc_name: &'a str) -> Option<Vec<TopicPartition<'a>>> {
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

    impl<K, V> Producer for MockProducer<K, V>
        where K: Hash + Clone,
              V: Clone
    {
        type Key = K;
        type Value = V;

        fn partitions_for<'a>(&self, topic_name: &'a str) -> Option<Vec<TopicPartition<'a>>> {
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
use std::mem;
use std::hash::Hash;
use std::time::Duration;
use std::net::SocketAddr;

use time;

use futures::future;
use tokio_core::reactor::Handle;

use protocol::{Message, MessageSet, MessageTimestamp};
use client::{Client, Cluster, KafkaClient, ToMilliseconds, TopicPartition};
use producer::{FlushProducer, Partitioner, Producer, ProducerBuilder, ProducerConfig,
               ProducerRecord, RecordMetadata, SendRecord, Serializer};

pub struct KafkaProducer<'a, K, V, P> {
    client: KafkaClient<'a>,
    config: ProducerConfig,
    key_serializer: K,
    value_serializer: V,
    partitioner: P,
}

impl<'a, K, V, P> KafkaProducer<'a, K, V, P> {
    pub fn new(client: KafkaClient<'a>,
               config: ProducerConfig,
               key_serializer: K,
               value_serializer: V,
               partitioner: P)
               -> Self {
        KafkaProducer {
            client: client,
            config: config,
            key_serializer: key_serializer,
            value_serializer: value_serializer,
            partitioner: partitioner,
        }
    }

    pub fn from_client(client: KafkaClient<'a>) -> ProducerBuilder<'a, K, V, P> {
        ProducerBuilder::from_client(client)
    }

    pub fn from_hosts<I>(hosts: I, handle: Handle) -> ProducerBuilder<'a, K, V, P>
        where I: Iterator<Item = SocketAddr>
    {
        ProducerBuilder::from_config(ProducerConfig::from_hosts(hosts), handle)
    }

    pub fn client(&self) -> &KafkaClient<'a> {
        &self.client
    }

    pub fn into_client(self) -> KafkaClient<'a> {
        self.client
    }
}

impl<'a, K, V, P> Producer<'a> for KafkaProducer<'a, K, V, P>
    where K: Serializer,
          K::Item: Hash,
          V: Serializer,
          P: Partitioner,
          Self: 'static
{
    type Key = K::Item;
    type Value = V::Item;

    fn partitions_for(&'a self, toipc_name: &'a str) -> Option<Vec<TopicPartition<'a>>> {
        self.client.metadata().partitions_for_topic(toipc_name)
    }

    fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord {
        let compression = self.config.compression;
        let timestamp = record
            .timestamp
            .unwrap_or_else(|| time::now_utc().to_timespec().as_millis() as i64);

        let message_set = MessageSet {
            messages: vec![Message {
                               offset: 0,
                               timestamp: Some(MessageTimestamp::CreateTime(timestamp)),
                               compression: compression,
                               key: None,
                               value: None,
                           }],
        };

        //self.partitioner.partition(self, &mut record);

        let produce =
            self.client
                .produce_records(self.config.acks,
                                 Duration::from_millis(self.config.ack_timeout),
                                 vec![(record.topic_name.to_owned(),
                                       vec![(record.partition.unwrap_or_default(), message_set)])]);

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

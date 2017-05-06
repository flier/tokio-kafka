use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;

use time;

use bytes::Bytes;
use bytes::buf::FromBuf;

use futures::{Future, future};
use tokio_core::reactor::Handle;

use errors::ErrorKind;
use protocol::{KafkaCode, Message, MessageSet, MessageTimestamp};
use network::BatchRecord;
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

    pub fn client(&mut self) -> &mut KafkaClient<'a> {
        &mut self.client
    }

    pub fn into_client(self) -> KafkaClient<'a> {
        self.client
    }
}

impl<'a, K, V, P> Producer<'a> for KafkaProducer<'a, K, V, P>
    where K: Serializer,
          K::Item: Debug + Hash,
          V: Serializer,
          V::Item: Debug,
          P: Partitioner,
          Self: 'static
{
    type Key = K::Item;
    type Value = V::Item;

    fn partitions_for(&self, toipc_name: String) -> Option<Vec<TopicPartition>> {
        self.client.metadata().partitions_for_topic(toipc_name)
    }

    fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord {
        trace!("sending {:?}", record);

        let ProducerRecord {
            topic_name,
            partition,
            key,
            value,
            timestamp,
        } = record;

        let partition = self.partitioner
            .partition(&topic_name,
                       partition,
                       key.as_ref(),
                       value.as_ref(),
                       self.client.metadata())
            .unwrap_or_default();

        let key = key.map(|ref key| {
                              let mut buf = Vec::with_capacity(16);
                              let _ = self.key_serializer
                                  .serialize(&topic_name, key, &mut buf)
                                  .map_err(|err| warn!("fail to serialize key, {}", err));
                              Bytes::from_buf(buf)
                          });
        let serialized_key_size = key.as_ref().map_or(0, |bytes| bytes.len());

        let value = value.map(|ref value| {
            let mut buf = Vec::with_capacity(16);
            let _ = self.value_serializer
                .serialize(&topic_name, value, &mut buf)
                .map_err(|err| warn!("fail to serialize value, {}", err));
            Bytes::from_buf(buf)
        });

        let serialized_value_size = value.as_ref().map_or(0, |bytes| bytes.len());

        let timestamp =
            timestamp.unwrap_or_else(|| time::now_utc().to_timespec().as_millis() as i64);

        let message_set = MessageSet {
            messages: vec![Message {
                               offset: 0,
                               timestamp: Some(MessageTimestamp::CreateTime(timestamp)),
                               compression: self.config.compression,
                               key: key,
                               value: value,
                           }],
        };

        let batch = BatchRecord {
            topic_name: topic_name.clone(),
            message_sets: vec![(partition, message_set)],
        };

        let produce = self.client
            .produce_records(self.config.acks, self.config.ack_timeout(), vec![batch])
            .and_then(move |responses| if let Some(partitions) = responses.get(&topic_name) {
                          let result =
                              partitions
                                  .iter()
                                  .find(|&&(partition_id, _, _)| partition_id == partition);

                          if let Some(&(partition_id, error_code, offset)) = result {
                              if error_code == KafkaCode::None as i16 {
                                  future::ok(RecordMetadata {
                                                 topic_name: topic_name,
                                                 partition: partition_id,
                                                 offset: offset,
                                                 timestamp: timestamp,
                                                 serialized_key_size: serialized_key_size,
                                                 serialized_value_size: serialized_value_size,
                                             })
                              } else {
                                  future::err(ErrorKind::KafkaError(KafkaCode::from(error_code))
                                                  .into())
                              }
                          } else {
                              future::err("no response for partition".into())
                          }
                      } else {
                          future::err("no response for topic".into())
                      });

        SendRecord::new(produce)
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
        pub records: Vec<(Option<K>, Option<V>)>,
    }

    impl<'a, K, V> Producer<'a> for MockProducer<K, V>
        where K: Hash + Clone,
              V: Clone
    {
        type Key = K;
        type Value = V;

        fn partitions_for(&self, topic_name: String) -> Option<Vec<TopicPartition>> {
            self.topics
                .get(&topic_name)
                .map(move |partitions| {
                    partitions
                        .iter()
                        .map(|&(_, partition)| {
                                 TopicPartition {
                                     topic_name: topic_name.clone(),
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

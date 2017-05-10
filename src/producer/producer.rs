use std::rc::Rc;
use std::cell::RefCell;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use std::net::SocketAddr;

use time;

use bytes::Bytes;
use bytes::buf::FromBuf;

use futures::{Future, Stream, future};
use tokio_core::reactor::Handle;

use protocol::ApiKeys;
use network::TopicPartition;
use client::{Client, Cluster, KafkaClient, ToMilliseconds};
use producer::{Accumulator, FlushProducer, Partitioner, Producer, ProducerBuilder, ProducerConfig,
               ProducerRecord, RecordAccumulator, SendRecord, Serializer};

pub struct KafkaProducer<'a, K, V, P> {
    client: Rc<RefCell<KafkaClient<'a>>>,
    config: ProducerConfig,
    accumulators: RecordAccumulator<'a>,
    key_serializer: K,
    value_serializer: V,
    partitioner: P,
}

impl<'a, K, V, P> KafkaProducer<'a, K, V, P>
    where Self: 'static
{
    pub fn new(client: KafkaClient<'a>,
               config: ProducerConfig,
               key_serializer: K,
               value_serializer: V,
               partitioner: P)
               -> Self {
        let accumulators = RecordAccumulator::new(config.batch_size,
                                                  config.compression,
                                                  Duration::from_millis(config.linger),
                                                  Duration::from_millis(config.retry_backoff));
        let handle = client.handle().clone();
        let client = Rc::new(RefCell::new(client));
        {
            let client = client.clone();
            let acks = config.acks;
            let ack_timeout = config.ack_timeout();

            handle.spawn(accumulators
                             .batches()
                             .for_each(move |(tp, batch)| {
                                           let records = vec![(tp, batch.build())];
                                           client
                                               .borrow_mut()
                                               .produce_records(acks, ack_timeout, records)
                                               .and_then(|responses| Ok(()))

                                       })
                             .map_err(|err| {
                                          warn!("fail to send records, {}", err);
                                          ()
                                      }));
        }

        KafkaProducer {
            client: client,
            config: config,
            accumulators: accumulators,
            key_serializer: key_serializer,
            value_serializer: value_serializer,
            partitioner: partitioner,
        }
    }

    pub fn client(&self) -> Rc<RefCell<KafkaClient<'a>>> {
        self.client.clone()
    }

    pub fn from_client(client: KafkaClient<'a>) -> ProducerBuilder<'a, K, V, P> {
        ProducerBuilder::from_client(client)
    }

    pub fn from_hosts<I>(hosts: I, handle: Handle) -> ProducerBuilder<'a, K, V, P>
        where I: Iterator<Item = SocketAddr>
    {
        ProducerBuilder::from_config(ProducerConfig::from_hosts(hosts), handle)
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

    fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord {
        trace!("sending {:?}", record);

        let ProducerRecord {
            topic_name,
            partition,
            key,
            value,
            timestamp,
        } = record;

        let cluster = self.client.borrow().metadata();

        let partition = self.partitioner
            .partition(&topic_name,
                       partition,
                       key.as_ref(),
                       value.as_ref(),
                       cluster.clone())
            .unwrap_or_default();

        let key = key.map(|ref key| {
                              let mut buf = Vec::with_capacity(16);
                              let _ = self.key_serializer
                                  .serialize(&topic_name, key, &mut buf)
                                  .map_err(|err| warn!("fail to serialize key, {}", err));
                              Bytes::from_buf(buf)
                          });

        let value = value.map(|ref value| {
            let mut buf = Vec::with_capacity(16);
            let _ = self.value_serializer
                .serialize(&topic_name, value, &mut buf)
                .map_err(|err| warn!("fail to serialize value, {}", err));
            Bytes::from_buf(buf)
        });

        let timestamp =
            timestamp.unwrap_or_else(|| time::now_utc().to_timespec().as_millis() as i64);

        let tp = TopicPartition {
            topic_name: topic_name.into(),
            partition: partition,
        };

        let api_version = cluster
            .leader_for(&tp)
            .and_then(|broker| broker.api_versions())
            .and_then(|api_versions| api_versions.find(ApiKeys::Produce))
            .map_or(0, |api_version| api_version.max_version);

        SendRecord::new(self.accumulators
                            .push_record(tp, timestamp, key, value, api_version))
        // let message_set = MessageSet {
        // messages: vec![Message {
        // offset: 0,
        // timestamp: Some(MessageTimestamp::CreateTime(timestamp)),
        // compression: self.config.compression,
        // key: key,
        // value: value,
        // }],
        // };
        //
        // let batch = BatchRecord {
        // topic_name: topic_name.clone(),
        // message_sets: vec![(partition, message_set)],
        // };
        //
        // let produce = self.client
        // .produce_records(self.config.acks, self.config.ack_timeout(), vec![batch])
        // .and_then(move |responses| if let Some(partitions) = responses.get(&topic_name) {
        // let result =
        // partitions
        // .iter()
        // .find(|&&(partition_id, _, _)| partition_id == partition);
        //
        // if let Some(&(partition_id, error_code, offset)) = result {
        // if error_code == KafkaCode::None as i16 {
        // future::ok(RecordMetadata {
        // topic_name: topic_name,
        // partition: partition_id,
        // offset: offset,
        // timestamp: timestamp,
        // serialized_key_size: serialized_key_size,
        // serialized_value_size: serialized_value_size,
        // })
        // } else {
        // future::err(ErrorKind::KafkaError(KafkaCode::from(error_code))
        // .into())
        // }
        // } else {
        // future::err("no response for partition".into())
        // }
        // } else {
        // future::err("no response for topic".into())
        // });
        //
        // SendRecord::new(produce)
        //
    }

    fn flush(&mut self) -> FlushProducer {
        FlushProducer::new(future::ok(()))
    }
}

#[cfg(test)]
pub mod mock {
    use std::mem;
    use std::hash::Hash;

    use futures::future;

    use producer::{FlushProducer, Producer, ProducerRecord, RecordMetadata, SendRecord};

    #[derive(Debug, Default)]
    pub struct MockProducer<K, V>
        where K: Hash
    {
        pub records: Vec<(Option<K>, Option<V>)>,
    }

    impl<'a, K, V> Producer<'a> for MockProducer<K, V>
        where K: Hash + Clone,
              V: Clone
    {
        type Key = K;
        type Value = V;

        fn send(&mut self, record: ProducerRecord<Self::Key, Self::Value>) -> SendRecord {
            self.records.push((record.key, record.value));

            SendRecord::new(future::ok(RecordMetadata { ..unsafe { mem::zeroed() } }))
        }

        fn flush(&mut self) -> FlushProducer {
            FlushProducer::new(future::ok(()))
        }
    }
}

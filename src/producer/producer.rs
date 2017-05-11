use std::rc::Rc;
use std::cell::RefCell;
use std::borrow::Borrow;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use std::net::SocketAddr;

use time;

use futures::{Future, Stream};
use tokio_core::reactor::Handle;

use errors::Result;
use protocol::ApiKeys;
use network::TopicPartition;
use client::{Client, Cluster, KafkaClient, Metadata, ToMilliseconds};
use producer::{Accumulator, Partitioner, Producer, ProducerBuilder, ProducerConfig,
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

            let produce = accumulators
                .batches()
                .for_each(move |(tp, batch)| {
                    let topic_name: String = String::from(tp.topic_name.borrow());
                    let partition = tp.partition;
                    let (thunks, message_set) = batch.build();

                    client
                        .borrow_mut()
                        .produce_records(acks, ack_timeout, vec![(tp, message_set)])
                        .map(move |responses| if let Some(partitions) =
                            responses.get(&topic_name.to_owned()) {
                                 partitions
                                     .iter()
                                     .find(|&&(partition_id, _, _)| partition_id == partition)
                                     .map(|&(_, error_code, offset)| for thunk in thunks {
                                              match thunk.send(&topic_name,
                                                               partition,
                                                               offset,
                                                               error_code.into()) {
                                                  Ok(()) => {}
                                                  Err(metadata) => {
                                                      warn!("fail to send record metadata, {:?}",
                                                            metadata)
                                                  }
                                              }
                                          });
                             })
                })
                .map_err(|err| {
                             warn!("fail to produce records, {}", err);
                             ()
                         });

            handle.spawn(produce);
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
        trace!("sending record {:?}", record);

        let ProducerRecord {
            topic_name,
            partition,
            key,
            value,
            timestamp,
        } = record;

        let client: &RefCell<KafkaClient> = self.client.borrow();
        let cluster: Rc<Metadata> = client.borrow().metadata();

        let partition = self.partitioner
            .partition(&topic_name,
                       partition,
                       key.as_ref(),
                       value.as_ref(),
                       cluster.clone())
            .unwrap_or_default();

        let key = key.and_then(|key| {
                                   self.key_serializer
                                       .serialize(&topic_name, key)
                                       .map_err(|err| warn!("fail to serialize key, {}", err))
                                       .ok()
                               });

        let value =
            value.and_then(|value| {
                               self.value_serializer
                                   .serialize(&topic_name, value)
                                   .map_err(|err| warn!("fail to serialize value, {}", err))
                                   .ok()
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
    }

    fn flush(&mut self) -> Result<()> {
        self.accumulators.flush()
    }
}

#[cfg(test)]
pub mod mock {
    use std::mem;
    use std::hash::Hash;

    use futures::future;

    use errors::Result;
    use producer::{Producer, ProducerRecord, RecordMetadata, SendRecord};

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

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }
}

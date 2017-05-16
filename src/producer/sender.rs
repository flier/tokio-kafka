use std::rc::Rc;
use std::cell::RefCell;
use std::hash::Hash;
use std::borrow::{Borrow, Cow};
use std::time::Duration;

use futures::Future;

use errors::Result;
use protocol::{MessageSet, RequiredAcks};
use network::TopicPartition;
use client::{Client, KafkaClient, StaticBoxFuture};
use producer::{Interceptors, ProducerBatch, Thunk};

pub struct Sender<'a, K, V> {
    client: Rc<KafkaClient<'a>>,
    interceptors: Interceptors<K, V>,
    acks: RequiredAcks,
    ack_timeout: Duration,
    tp: TopicPartition<'a>,
    thunks: Rc<RefCell<Option<Vec<Thunk>>>>,
    message_set: MessageSet,
}

pub type SendBatch = StaticBoxFuture;

impl<'a, K, V> Sender<'a, K, V>
    where K: Hash,
          Self: 'static
{
    pub fn new(client: Rc<KafkaClient<'a>>,
               interceptors: Interceptors<K, V>,
               acks: RequiredAcks,
               ack_timeout: Duration,
               tp: TopicPartition<'a>,
               batch: ProducerBatch)
               -> Result<Sender<'a, K, V>> {
        let (thunks, message_set) = batch.build()?;

        Ok(Sender {
               client: client,
               interceptors: interceptors,
               acks: acks,
               ack_timeout: ack_timeout,
               tp: tp,
               thunks: Rc::new(RefCell::new(Some(thunks))),
               message_set: message_set,
           })
    }

    pub fn send_batch(&self) -> SendBatch {
        trace!("sending batch to {:?}: {:?}", self.tp, self.message_set);

        let topic_name: String = String::from(self.tp.topic_name.borrow());
        let partition = self.tp.partition;
        let acks = self.acks;
        let ack_timeout = self.ack_timeout;
        let message_set = Cow::Owned(self.message_set.clone());
        let thunks = self.thunks.clone();
        let interceptors = self.interceptors.clone();

        let send_batch = (*self.client)
            .borrow()
            .produce_records(acks,
                             ack_timeout,
                             TopicPartition {
                                 topic_name: topic_name.clone().into(),
                                 partition: partition,
                             },
                             vec![message_set])
            .map(move |responses| {
                responses
                    .get(&topic_name)
                    .map(|partitions| {
                        partitions
                            .iter()
                            .find(|&&(partition_id, _, _)| partition_id == partition)
                            .map(|&(_, error_code, offset)| if let Some(thunks) = (*thunks)
                                        .borrow_mut()
                                        .take() {
                                     for thunk in thunks {
                                         match thunk.done(interceptors.clone(),
                                                          &topic_name,
                                                          partition,
                                                          offset,
                                                          error_code.into()) {
                                             Ok(()) => {}
                                             Err(metadata) => {
                                                 warn!("fail to send record metadata, {:?}",
                                                       metadata)
                                             }
                                         }
                                     }
                                 });
                    });
            });

        SendBatch::new(send_batch)
    }
}

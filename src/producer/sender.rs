use std::borrow::{Borrow, Cow};
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use std::time::Duration;

use futures::Future;

use client::{Client, KafkaClient, StaticBoxFuture, ToStaticBoxFuture};
use errors::Result;
use network::TopicPartition;
use producer::{Interceptors, ProducerBatch, Thunk};
use protocol::{MessageSet, RequiredAcks};

pub struct Sender<'a, K, V> {
    client: KafkaClient<'a>,
    interceptors: Interceptors<K, V>,
    acks: RequiredAcks,
    ack_timeout: Duration,
    tp: TopicPartition<'a>,
    thunks: Rc<RefCell<Option<Vec<Thunk>>>>,
    message_set: MessageSet,
}

pub type SendBatch = StaticBoxFuture;

impl<'a, K, V> Sender<'a, K, V>
where
    K: Hash,
    Self: 'static,
{
    pub fn new(
        client: KafkaClient<'a>,
        interceptors: Interceptors<K, V>,
        acks: RequiredAcks,
        ack_timeout: Duration,
        tp: TopicPartition<'a>,
        batch: ProducerBatch,
    ) -> Result<Sender<'a, K, V>> {
        let (thunks, message_set) = batch.build()?;

        Ok(Sender {
            client,
            interceptors,
            acks,
            ack_timeout,
            tp,
            thunks: Rc::new(RefCell::new(Some(thunks))),
            message_set,
        })
    }

    pub fn send_batch(&self) -> SendBatch {
        trace!("sending batch to {:?}: {:?}", self.tp, self.message_set);

        let topic_name: String = String::from(self.tp.topic_name.borrow());
        let partition_id = self.tp.partition_id;
        let acks = self.acks;
        let ack_timeout = self.ack_timeout;
        let message_set = Cow::Owned(self.message_set.clone());
        let thunks = self.thunks.clone();
        let thunks1 = self.thunks.clone();
        let interceptors = self.interceptors.clone();

        self.client
            .produce_records(
                acks,
                ack_timeout,
                topic_partition!(topic_name.clone(), partition_id),
                vec![message_set],
            )
            .map(move |responses| {
                if let Some(partitions) = responses.get(&topic_name) {
                    if let Some(partition) = partitions
                        .iter()
                        .find(|partition| partition.partition_id == partition_id)
                    {
                        if let Some(thunks) = (*thunks).borrow_mut().take() {
                            for thunk in thunks {
                                match thunk.done(
                                    interceptors.clone(),
                                    &topic_name,
                                    partition.partition_id,
                                    partition.base_offset,
                                    partition.error_code,
                                ) {
                                    Ok(()) => {}
                                    Err(metadata) => warn!("fail to send record metadata, {:?}", metadata),
                                }
                            }
                        }
                    }
                }
            })
            .map_err(move |err| {
                if let Some(thunks) = (*thunks1).borrow_mut().take() {
                    for thunk in thunks {
                        if let Err(err) = thunk.fail(format!("{}", err).into()) {
                            warn!("fail to send error to thunk, {:?}", err);
                        }
                    }
                }
                err
            })
            .static_boxed()
    }
}

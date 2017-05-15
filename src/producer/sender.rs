use std::rc::Rc;
use std::cell::{Ref, RefCell};
use std::borrow::{Borrow, Cow};
use std::time::Duration;

use futures::{Future, Poll};

use errors::{Error, Result};
use protocol::{MessageSet, RequiredAcks};
use network::TopicPartition;
use client::{Client, KafkaClient, StaticBoxFuture};
use producer::{ProducerBatch, Thunk};

pub struct Sender<'a> {
    inner: Rc<RefCell<SenderInner<'a>>>,
}

struct SenderInner<'a> {
    client: Rc<RefCell<KafkaClient<'a>>>,
    acks: RequiredAcks,
    ack_timeout: Duration,
    tp: TopicPartition<'a>,
    thunks: Rc<RefCell<Option<Vec<Thunk>>>>,
    message_set: MessageSet,
}

impl<'a> Sender<'a>
    where Self: 'static
{
    pub fn new(client: Rc<RefCell<KafkaClient<'a>>>,
               acks: RequiredAcks,
               ack_timeout: Duration,
               tp: TopicPartition<'a>,
               batch: ProducerBatch)
               -> Result<Sender<'a>> {
        let (thunks, message_set) = batch.build()?;
        let inner = SenderInner {
            client: client,
            acks: acks,
            ack_timeout: ack_timeout,
            tp: tp,
            thunks: Rc::new(RefCell::new(Some(thunks))),
            message_set: message_set,
        };
        Ok(Sender { inner: Rc::new(RefCell::new(inner)) })
    }

    fn as_inner(&self) -> Ref<SenderInner<'a>> {
        let inner: &RefCell<SenderInner> = self.inner.borrow();

        inner.borrow()
    }

    pub fn send_batch(&self) -> SendBatch<'a> {
        let inner = self.inner.clone();

        let send_batch = self.as_inner().send_batch();

        SendBatch::new(inner, StaticBoxFuture::new(send_batch))
    }
}

impl<'a> SenderInner<'a>
    where Self: 'static
{
    pub fn as_client(&self) -> Ref<KafkaClient<'a>> {
        let client: &RefCell<KafkaClient> = self.client.borrow();

        client.borrow()
    }

    pub fn send_batch(&self) -> StaticBoxFuture {
        trace!("sending batch to {:?}: {:?}", self.tp, self.message_set);

        let topic_name: String = String::from(self.tp.topic_name.borrow());
        let partition = self.tp.partition;
        let acks = self.acks;
        let ack_timeout = self.ack_timeout;
        let message_set = Cow::Owned(self.message_set.clone());
        let thunks = self.thunks.clone();

        let send_batch = self.as_client()
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
                            .map(|&(_, error_code, offset)| {
                                let thunks: &RefCell<Option<Vec<Thunk>>> = thunks.borrow();

                                if let Some(thunks) = thunks.borrow_mut().take() {
                                    for thunk in thunks {
                                        match thunk.done(&topic_name,
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
                                }
                            });
                    });
            });

        StaticBoxFuture::new(send_batch)
    }
}

pub struct SendBatch<'a> {
    inner: Rc<RefCell<SenderInner<'a>>>,
    future: StaticBoxFuture,
}

impl<'a> SendBatch<'a> {
    fn new(inner: Rc<RefCell<SenderInner<'a>>>, future: StaticBoxFuture) -> SendBatch<'a> {
        SendBatch {
            inner: inner,
            future: future,
        }
    }
}

impl<'a> Future for SendBatch<'a> {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.future.poll()
    }
}

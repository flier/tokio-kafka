use std::rc::Rc;
use std::cell::RefCell;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

use errors::Result;

use producer::{ProducerRecord, RecordMetadata};

pub type Interceptors<K, V> = Option<Rc<RefCell<ProducerInterceptors<K, V>>>>;

/// A trait for intercepting (and possibly mutate) the records
/// received by the producer before they are published to the Kafka cluster.
pub trait ProducerInterceptor {
    /// The type of key
    type Key: Hash;
    /// The type of value
    type Value;

    /// This is called from [`KafkaProducer::send`](struct.KafkaProducer.html#send.v) method,
    /// before key and value get serialized and partition is assigned
    /// (if partition is not specified in ProducerRecord).
    fn send(&self,
            record: ProducerRecord<Self::Key, Self::Value>)
            -> Result<ProducerRecord<Self::Key, Self::Value>>;

    /// This method is called when the record sent to the server has been acknowledged,
    /// or when sending the record fails before it gets sent to the server.
    fn ack(&self, result: &Result<RecordMetadata>);
}

pub struct ProducerInterceptors<K, V> {
    interceptors: Vec<Box<ProducerInterceptor<Key = K, Value = V>>>,
}

impl<K, V> Deref for ProducerInterceptors<K, V> {
    type Target = Vec<Box<ProducerInterceptor<Key = K, Value = V>>>;

    fn deref(&self) -> &Self::Target {
        &self.interceptors
    }
}

impl<K, V> DerefMut for ProducerInterceptors<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.interceptors
    }
}

impl<K, V> Default for ProducerInterceptors<K, V> {
    fn default() -> Self {
        ProducerInterceptors { interceptors: Vec::new() }
    }
}

impl<K, V> ProducerInterceptors<K, V> {
    pub fn new() -> Self {
        ProducerInterceptors::default()
    }
}

impl<K, V> ProducerInterceptor for ProducerInterceptors<K, V>
    where K: Hash
{
    type Key = K;
    type Value = V;

    fn send(&self, mut record: ProducerRecord<K, V>) -> Result<ProducerRecord<K, V>> {
        for interceptor in &self.interceptors {
            record = interceptor.send(record)?;
        }

        Ok(record)
    }

    fn ack(&self, result: &Result<RecordMetadata>) {
        for interceptor in &self.interceptors {
            interceptor.ack(result);
        }
    }
}

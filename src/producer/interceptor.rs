use std::hash::Hash;
use std::ops::{Deref, DerefMut};

use errors::Result;

use producer::{ProducerRecord, RecordMetadata};

/// A plugin interface that allows you to intercept (and possibly mutate)
/// the records received by the producer before they are published to the Kafka cluster.
pub trait ProducerInterceptor {
    type Key: Hash;
    type Value;

    fn send(&self,
            record: ProducerRecord<Self::Key, Self::Value>)
            -> Result<ProducerRecord<Self::Key, Self::Value>> {
        Ok(record)
    }

    fn ack(&self, _result: &Result<RecordMetadata>) {}
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

impl<K, V> ProducerInterceptors<K, V> {
    pub fn new() -> Self {
        ProducerInterceptors { interceptors: vec![] }
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

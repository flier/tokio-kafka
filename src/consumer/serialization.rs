use std::marker::PhantomData;

use errors::Error;

/// A trait for deserializing type from Kafka record
pub trait Deserializer {
    /// The type of value that this deserializer will deserialize.
    type Item;
    /// The type of error that this deserializer will return if it fails.
    type Error;
}

/// Deserialize type from nothing
#[derive(Clone, Debug, Default)]
pub struct NoopDeserializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Deserializer for NoopDeserializer<T> {
    type Item = T;
    type Error = Error;
}

/// Deserialize type from it's raw data
#[derive(Clone, Debug, Default)]
pub struct RawDeserializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Deserializer for RawDeserializer<T> {
    type Item = T;
    type Error = Error;
}

/// Deserialize `Buf` like type from it's raw bytes
#[derive(Clone, Debug, Default)]
pub struct BytesDeserializer<T> {
    phantom: PhantomData<T>,
}

impl<T> Deserializer for BytesDeserializer<T> {
    type Item = T;
    type Error = Error;
}

/// A trait for deserializing type from Kafka record
pub trait Deserializer {
    /// The type of value that this deserializer will deserialize.
    type Item;
    /// The type of error that this deserializer will return if it fails.
    type Error;
}

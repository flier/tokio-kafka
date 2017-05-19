use tokio_core::reactor::Handle;

use client::KafkaClient;
use consumer::{ConsumerConfig, Deserializer};

/// A `KafkaConsumer` builder easing the process of setting up various configuration settings.
pub struct ConsumerBuilder<'a, K, V>
    where K: Deserializer,
          V: Deserializer
{
    config: ConsumerConfig,
    client: Option<KafkaClient<'a>>,
    handle: Option<Handle>,
    key_deserializer: Option<K>,
    value_deserializer: Option<V>,
}

use std::time::Duration;
use std::net::{SocketAddr, ToSocketAddrs};

use compression::Compression;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
    #[serde(rename = "bootstrap.servers")]
    pub hosts: Vec<SocketAddr>,

    /// Client identifier.
    #[serde(rename = "client.id")]
    pub client_id: Option<String>,

    /// Compression codec to use for compressing message sets.
    #[serde(rename = "compression.codec")]
    pub compression: Compression,

    /// Maximum connection idle timeout
    #[serde(rename = "connection.max.idle.ms")]
    pub max_connection_idle_ms: u64,

    /// Maximum pooled connections per broker
    #[serde(rename = "connection.max.pooled")]
    pub max_pooled_connections: usize,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        KafkaConfig {
            hosts: vec![],
            client_id: None,
            compression: Compression::None,
            max_connection_idle_ms: 5000,
            max_pooled_connections: 4,
        }
    }
}

impl KafkaConfig {
    pub fn from_hosts<A: ToSocketAddrs>(hosts: &[A]) -> Self {
        KafkaConfig {
            hosts: hosts
                .iter()
                .flat_map(|host| host.to_socket_addrs().unwrap())
                .collect(),
            ..Default::default()
        }
    }

    pub fn max_connection_idle(&self) -> Duration {
        Duration::new((self.max_connection_idle_ms / 1000) as u64,
                      (self.max_connection_idle_ms % 1000) as u32 * 1000_000)
    }
}

#[cfg(test)]
mod tests {
    extern crate serde_json;

    use super::*;

    #[test]
    fn test_serialize() {
        let config = KafkaConfig {
            hosts: vec!["127.0.0.1:9092".parse().unwrap()],
            client_id: Some("tokio-kafka".to_owned()),
            compression: Compression::Snappy,
            max_connection_idle_ms: 5000,
            max_pooled_connections: 4,
        };
        let json = r#"{
  "bootstrap.servers": [
    "127.0.0.1:9092"
  ],
  "client.id": "tokio-kafka",
  "compression.codec": "snappy",
  "connection.max.idle.ms": 5000,
  "connection.max.pooled": 4
}"#;

        assert_eq!(serde_json::to_string_pretty(&config).unwrap(), json);
        assert_eq!(serde_json::from_str::<KafkaConfig>(json).unwrap(), config);
    }
}
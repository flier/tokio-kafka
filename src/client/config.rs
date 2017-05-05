use std::time::Duration;
use std::net::SocketAddr;

use time::Timespec;

pub const DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS: u64 = 5000;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientConfig {
    /// A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
    #[serde(rename = "bootstrap.servers")]
    pub hosts: Vec<SocketAddr>,

    /// An id string to pass to the server when making requests.
    ///
    /// The purpose of this is to be able to track the source of requests beyond just ip/port
    /// by allowing a logical application name to be included in server-side request logging.
    #[serde(rename = "client.id")]
    pub client_id: Option<String>,

    /// Close idle connections after the number of milliseconds specified by this config.
    #[serde(rename = "connection.max.idle.ms")]
    pub max_connection_idle: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            hosts: vec![],
            client_id: None,
            max_connection_idle: DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS,
        }
    }
}

impl ClientConfig {
    pub fn from_hosts<I>(hosts: I) -> Self
        where I: Iterator<Item = SocketAddr>
    {
        ClientConfig {
            hosts: hosts.collect(),
            ..Default::default()
        }
    }
}

impl ClientConfig {
    pub fn max_connection_idle(&self) -> Duration {
        Duration::new((self.max_connection_idle / 1000) as u64,
                      (self.max_connection_idle % 1000) as u32 * 1000_000)
    }
}

pub trait ToMilliseconds {
    fn as_millis(&self) -> u64;
}

impl ToMilliseconds for Duration {
    fn as_millis(&self) -> u64 {
        self.as_secs() * 1000 + self.subsec_nanos() as u64 / 1000_000
    }
}

impl ToMilliseconds for Timespec {
    fn as_millis(&self) -> u64 {
        self.sec as u64 * 1000 + self.nsec as u64 / 1000_000
    }
}

#[cfg(test)]
mod tests {
    extern crate serde_json;

    use super::*;

    #[test]
    fn test_serialize() {
        let config = ClientConfig {
            hosts: vec!["127.0.0.1:9092".parse().unwrap()],
            client_id: Some("tokio-kafka".to_owned()),
            max_connection_idle: 5000,
        };
        let json = r#"{
  "bootstrap.servers": [
    "127.0.0.1:9092"
  ],
  "client.id": "tokio-kafka",
  "connection.max.idle.ms": 5000
}"#;

        assert_eq!(serde_json::to_string_pretty(&config).unwrap(), json);
        assert_eq!(serde_json::from_str::<ClientConfig>(json).unwrap(), config);
    }
}

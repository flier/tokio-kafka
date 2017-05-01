use std::time::Duration;
use std::ops::{Deref, DerefMut};
use std::net::{SocketAddr, ToSocketAddrs};

use compression::Compression;
use protocol::RequiredAcks;

pub const DEFAULT_MAX_POOLED_CONNECTIONS: usize = 4;

lazy_static!{
    pub static ref DEFAULT_MAX_CONNECTION_TIMEOUT: Duration = Duration::from_secs(15 * 60);
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum KafkaOption {
    /// Initial list of brokers (host or host:port)
    #[serde(rename = "bootstrap.servers")]
    Brokers(Vec<SocketAddr>),

    /// Client identifier.
    #[serde(rename = "client.id")]
    ClientId(String),

    /// Compression codec to use for compressing message sets.
    #[serde(rename = "compression.codec")]
    CompressionCodec(Compression),

    /// Maximum connection idle timeout
    #[serde(rename = "connection.max.idle.ms")]
    MaxConnectionIdle(u64),

    /// Maximum pooled connections per broker
    #[serde(rename = "connection.max.pooled")]
    MaxPooledConnections(usize),

    /// How many acknowledgements the leader broker must receive
    /// from ISR brokers before responding to the request
    #[serde(rename = "request.required.acks")]
    Acks(RequiredAcks),
}

macro_rules! get_property {
    ($opts:ident, $variant:path) => ({
        $opts.0
            .iter()
            .flat_map(|prop| if let &$variant(ref value) = prop {
                          Some(value.clone())
                      } else {
                          None
                      })
            .next()
    })
}

macro_rules! set_property {
    ($opts:ident, $variant:path => $value:expr) => ({
        let idx = $opts.0
            .iter()
            .position(|prop| if let &$variant(..) = prop {
                          true
                      } else {
                          false
                      });

        if let Some(idx) = idx {
            $opts.0.remove(idx);
        }

        $opts.0.push($variant($value));
        $opts
    })
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KafkaConfig(Vec<KafkaOption>);

impl<T> From<T> for KafkaConfig
    where T: IntoIterator<Item = KafkaOption>
{
    fn from(opts: T) -> Self {
        KafkaConfig(opts.into_iter().collect())
    }
}

impl Deref for KafkaConfig {
    type Target = [KafkaOption];

    fn deref(&self) -> &Self::Target {
        &self.0.as_slice()
    }
}

impl DerefMut for KafkaConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut_slice()
    }
}

impl KafkaConfig {
    pub fn from_hosts<A: ToSocketAddrs>(hosts: &[A]) -> Self {
        KafkaConfig(vec![KafkaOption::Brokers(hosts
                                                  .iter()
                                                  .flat_map(|host| {
                                                                host.to_socket_addrs().unwrap()
                                                            })
                                                  .collect())])
    }

    pub fn brokers(&self) -> Option<Vec<SocketAddr>> {
        get_property!(self, KafkaOption::Brokers)
    }

    pub fn client_id(&self) -> Option<String> {
        get_property!(self, KafkaOption::ClientId)
    }

    pub fn with_client_id(&mut self, client_id: &str) -> &mut Self {
        set_property!(self, KafkaOption::ClientId => client_id.to_owned())
    }

    pub fn compression_codec(&self) -> Option<Compression> {
        get_property!(self, KafkaOption::CompressionCodec)
    }

    pub fn with_compression_codec(&mut self, compression: Compression) -> &mut Self {
        set_property!(self, KafkaOption::CompressionCodec => compression)
    }

    pub fn max_connection_idle(&self) -> Option<Duration> {
        get_property!(self, KafkaOption::MaxConnectionIdle).map(Duration::from_millis)
    }

    pub fn with_max_connection_idle(&mut self, max_connection_idle: Duration) -> &mut Self {
        let d = max_connection_idle.as_secs() * 1000 +
                max_connection_idle.subsec_nanos() as u64 / 1000_000;

        set_property!(self, KafkaOption::MaxConnectionIdle => d)
    }

    pub fn max_pooled_connections(&self) -> Option<usize> {
        get_property!(self, KafkaOption::MaxPooledConnections)
    }

    pub fn with_max_pooled_connections(&mut self, max_pooled_connections: usize) -> &mut Self {
        set_property!(self, KafkaOption::MaxPooledConnections => max_pooled_connections)
    }

    pub fn required_acks(&self) -> Option<RequiredAcks> {
        get_property!(self, KafkaOption::Acks)
    }

    pub fn with_required_acks(&mut self, acks: RequiredAcks) -> &mut Self {
        set_property!(self, KafkaOption::Acks => acks)
    }
}

#[cfg(test)]
mod tests {
    extern crate serde_json;

    use super::*;

    #[test]
    fn test_config() {
        let mut config = KafkaConfig::from_hosts(&["127.0.0.1:9092".to_owned()]);

        assert_eq!(config.brokers(),
                   Some(vec!["127.0.0.1:9092".parse().unwrap()]));

        assert_eq!(config.with_client_id("kafka").client_id(),
                   Some("kafka".to_owned()));

        assert_eq!(config.compression_codec(), None);
        assert_eq!(config
                       .with_compression_codec(Compression::LZ4)
                       .compression_codec(),
                   Some(Compression::LZ4));

        assert_eq!(config
                       .with_required_acks(RequiredAcks::All)
                       .required_acks(),
                   Some(RequiredAcks::All));
    }

    #[test]
    fn test_serialize() {
        let config =
            KafkaConfig::from(vec![KafkaOption::Brokers(vec!["127.0.0.1:9092".parse().unwrap()]),
                                   KafkaOption::ClientId("tokio-kafka".to_owned()),
                                   KafkaOption::CompressionCodec(Compression::Snappy),
                                   KafkaOption::MaxConnectionIdle(5000),
                                   KafkaOption::MaxPooledConnections(100),
                                   KafkaOption::Acks(RequiredAcks::All)]);
        let json = r#"[
  {
    "bootstrap.servers": [
      "127.0.0.1:9092"
    ]
  },
  {
    "client.id": "tokio-kafka"
  },
  {
    "compression.codec": "snappy"
  },
  {
    "connection.max.idle.ms": 5000
  },
  {
    "connection.max.pooled": 100
  },
  {
    "request.required.acks": "all"
  }
]"#;

        assert_eq!(serde_json::to_string_pretty(&config).unwrap(), json);
        assert_eq!(serde_json::from_str::<KafkaConfig>(json).unwrap(), config);
    }
}
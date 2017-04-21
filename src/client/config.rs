use std::ops::{Deref, DerefMut};
use std::net::{SocketAddr, ToSocketAddrs};

use compression::Compression;

#[derive(Clone, Debug, PartialEq)]
pub enum KafkaOption {
    /// Initial list of brokers (host or host:port)
    Brokers(Vec<SocketAddr>),
    /// Client identifier.
    ClientId(String),
    /// Compression codec to use for compressing message sets.
    CompressionCodec(Compression),
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

#[derive(Clone, Debug, PartialEq)]
pub struct KafkaConfig(Vec<KafkaOption>);

impl<T> From<T> for KafkaConfig
    where T: Iterator<Item = KafkaOption>
{
    fn from(opts: T) -> Self {
        KafkaConfig(opts.collect())
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
                                                  .flat_map(|host| host.to_socket_addrs())
                                                  .flat_map(|addrs| addrs)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        let mut config = KafkaConfig::new(&["localhost"]);

        assert_eq!(config.brokers(), Some(vec!["localhost"]));

        assert_eq!(config.with_brokers(&["localhost:9092"]).brokers(),
                   Some(vec!["localhost:9092"]));
        assert_eq!(config.with_client_id("kafka").client_id(),
                   Some("kafka".to_owned()));


        assert_eq!(config.compression_codec(), None);
        assert_eq!(config
                       .with_compression_codec(Compression::LZ4)
                       .compression_codec(),
                   Some(Compression::LZ4));
    }
}
use std::net::ToSocketAddrs;

use compression::Compression;

#[derive(Clone, Debug, PartialEq)]
pub enum KafkaProperty<S>
    where S: ToSocketAddrs + Clone
{
    /// Client identifier.
    ClientId(String),
    /// Initial list of brokers (host or host:port)
    Brokers(Vec<S>),
    /// Compression codec to use for compressing message sets.
    CompressionCodec(Compression),
}

macro_rules! get_property {
    ($props:ident, $variant:path) => ({
        $props.0
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
    ($props:ident, $variant:path => $value:expr) => ({
        let idx = $props.0
            .iter()
            .position(|prop| if let &$variant(..) = prop {
                          true
                      } else {
                          false
                      });

        if let Some(idx) = idx {
            $props.0.remove(idx);
        }

        $props.0.push($variant($value));
        $props
    })
}

#[derive(Clone, Debug, PartialEq)]
pub struct KafkaConfig<S>(Vec<KafkaProperty<S>>) where S: ToSocketAddrs + Clone;

impl<'a, S> From<&'a [KafkaProperty<S>]> for KafkaConfig<S>
    where S: ToSocketAddrs + Clone
{
    fn from(props: &[KafkaProperty<S>]) -> Self {
        KafkaConfig(Vec::from(props))
    }
}

impl<S> KafkaConfig<S>
    where S: ToSocketAddrs + Clone
{
    pub fn new(hosts: &[S]) -> Self {
        KafkaConfig(vec![KafkaProperty::Brokers(hosts.to_vec())])
    }

    pub fn client_id(&self) -> Option<String> {
        get_property!(self, KafkaProperty::ClientId)
    }

    pub fn with_client_id(&mut self, client_id: &str) -> &mut Self {
        set_property!(self, KafkaProperty::ClientId => client_id.to_owned())
    }

    pub fn brokers(&self) -> Option<Vec<S>> {
        get_property!(self, KafkaProperty::Brokers)
    }

    pub fn with_brokers(&mut self, brokers: &[S]) -> &mut Self {
        set_property!(self, KafkaProperty::Brokers => brokers.to_vec())
    }

    pub fn compression_codec(&self) -> Option<Compression> {
        get_property!(self, KafkaProperty::CompressionCodec)
    }

    pub fn with_compression_codec(&mut self, compression: Compression) -> &mut Self {
        set_property!(self, KafkaProperty::CompressionCodec => compression)
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
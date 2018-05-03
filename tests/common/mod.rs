use std::env;

use failure::Error;
use pretty_env_logger;

use futures::Future;
use tokio_core::reactor::Core;
use tokio_kafka::{ClientConfig, ConsumerConfig, KafkaClient, KafkaConsumer, KafkaVersion, StringDeserializer};

const DEFAULT_BROKER: &str = "localhost:9092";

pub struct IntegrationTest {
    brokers: Vec<String>,
    client_id: Option<String>,
    kafka_version: KafkaVersion,
}

impl IntegrationTest {
    pub fn new() -> Result<Self, Error> {
        let brokers = match env::var("KAFKA_BROKERS") {
            Ok(s) => s.split(",").map(|s| s.to_owned()).collect(),
            Err(env::VarError::NotPresent) => vec![DEFAULT_BROKER.to_owned()],
            Err(err) => bail!(err),
        };
        let client_id = match env::var("KAFKA_CLIENT") {
            Ok(s) => Some(s),
            Err(env::VarError::NotPresent) => None,
            Err(err) => bail!(err),
        };
        let kafka_version = match env::var("KAFKA_VERSION") {
            Ok(s) => s.parse()?,
            _ => KafkaVersion::default(),
        };

        Ok(IntegrationTest {
            brokers,
            client_id,
            kafka_version,
        })
    }

    pub fn client_config(self) -> ClientConfig {
        let config = ClientConfig {
            hosts: self.brokers,
            client_id: self.client_id,
            broker_version_fallback: self.kafka_version,
            ..Default::default()
        };

        info!("connect kafka server with config: {:?}", config);

        config
    }

    pub fn consumer_config(self) -> ConsumerConfig {
        let config = ConsumerConfig {
            client: self.client_config(),
            ..Default::default()
        };

        info!("connect kafka server with config: {:?}", config);

        config
    }
}

pub fn run_as_client<'a, F, R, O, E>(op: F) -> Result<O, Error>
where
    F: FnOnce(KafkaClient<'static>) -> R,
    R: Future<Item = O, Error = E>,
    E: Into<Error>,
{
    let _ = pretty_env_logger::try_init();

    let tests = IntegrationTest::new()?;

    let mut core = Core::new()?;

    let client = KafkaClient::new(tests.client_config(), core.handle());

    let work = op(client).map_err(|err| err.into());

    core.run(work)
}

pub fn run_as_consumer<'a, F, R, O, E>(op: F) -> Result<O, Error>
where
    F: FnOnce(KafkaConsumer<'static, StringDeserializer<String>, StringDeserializer<String>>) -> R,
    R: Future<Item = O, Error = E>,
    E: Into<Error>,
{
    let _ = pretty_env_logger::try_init();

    let tests = IntegrationTest::new()?;

    let mut core = Core::new()?;

    let consumer = KafkaConsumer::with_config(tests.consumer_config(), core.handle())
        .with_key_deserializer(StringDeserializer::default())
        .with_value_deserializer(StringDeserializer::default())
        .build()?;

    let work = op(consumer).map_err(|err| err.into());

    core.run(work)
}

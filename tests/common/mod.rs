use std::env;

use failure::Error;
use pretty_env_logger;

use futures::Future;
use tokio_core::reactor::Core;
use tokio_kafka::{ClientConfig, KafkaClient};

const DEFAULT_BROKER: &str = "localhost:9092";

pub struct IntegrationTest {
    brokers: Vec<String>,
    client_id: Option<String>,
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

        Ok(IntegrationTest { brokers, client_id })
    }

    pub fn client_config(self) -> ClientConfig {
        let config = ClientConfig {
            hosts: self.brokers,
            client_id: self.client_id,
            ..Default::default()
        };

        info!("connect kafka server with config: {:?}", config);

        config
    }
}

pub fn run<'a, F, R, O, E>(op: F) -> Result<O, Error>
where
    F: FnOnce(KafkaClient<'static>) -> R,
    R: Future<Item = O, Error = E>,
    E: Into<Error>,
{
    pretty_env_logger::init();

    let tests = IntegrationTest::new()?;

    let config = tests.client_config();

    let mut core = Core::new()?;

    let client = KafkaClient::new(config, core.handle());

    let work = op(client).map_err(|err| err.into());

    core.run(work)
}

#[macro_use]
extern crate log;
extern crate getopts;
#[macro_use]
extern crate failure;
extern crate pretty_env_logger;

extern crate futures;
extern crate tokio_core;
extern crate tokio_kafka;

#[cfg(features = "integration_test")]
mod common;

#[cfg(features = "integration_test")]
mod tests {
    use futures::Future;

    use tokio_kafka::Client;

    use common;

    #[test]
    fn metadata() {
        common::run(|client| {
            client.metadata().and_then(|metadata| {
                info!("fetch metadata: {:?}", metadata);

                Ok(())
            })
        }).unwrap()
    }
}

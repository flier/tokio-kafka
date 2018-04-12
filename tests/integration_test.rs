#[macro_use]
extern crate log;
extern crate getopts;
#[macro_use]
extern crate failure;
extern crate pretty_env_logger;

extern crate futures;
extern crate tokio_core;
extern crate tokio_kafka;

#[cfg(feature = "integration_test")]
mod common;

#[cfg(feature = "integration_test")]
mod tests {
    use futures::Future;

    use tokio_kafka::{Client, Cluster};

    use common;

    #[test]
    fn metadata() {
        common::run(|client| {
            client.metadata().and_then(|metadata| {
                info!("fetch metadata: {:?}", metadata);

                assert!(!metadata.brokers().is_empty());

                let topics = metadata.topics();

                assert!(topics.contains_key("foo"));
                assert!(topics.contains_key("bar"));

                assert_eq!(topics["foo"].len(), 1);
                assert_eq!(topics["bar"].len(), 4);

                Ok(())
            })
        }).unwrap()
    }
}

#[macro_use]
extern crate log;
extern crate getopts;
#[macro_use]
extern crate failure;
extern crate pretty_env_logger;

extern crate futures;
extern crate tokio_core;
#[macro_use]
extern crate tokio_kafka;

#[cfg(feature = "integration_test")]
mod common;

#[cfg(feature = "integration_test")]
mod tests {
    use futures::Future;

    use tokio_kafka::{Client, Cluster, FetchOffset, KafkaCode, ListedOffset};

    use common;

    #[test]
    fn metadata() {
        common::run(|client| {
            client.metadata().and_then(move |metadata| {
                info!("fetch metadata: {:?}", metadata);

                assert!(!metadata.brokers().is_empty());
                let partitions = {
                    let topics = metadata.topics();

                    assert!(topics.contains_key("foo"));
                    assert!(topics.contains_key("bar"));

                    assert_eq!(topics["foo"].len(), 1);
                    assert_eq!(topics["bar"].len(), 4);

                    topics
                        .into_iter()
                        .flat_map(|(topic_name, partitions)| {
                            partitions.into_iter().flat_map(move |partition| {
                                let tp = topic_partition!(topic_name.to_owned(), partition.partition_id);

                                vec![(tp.clone(), FetchOffset::Earliest), (tp.clone(), FetchOffset::Latest)]
                            })
                        })
                        .collect()
                };

                client.list_offsets(partitions).map(|responses| {
                    assert!(responses.contains_key("foo"));
                    assert!(responses.contains_key("bar"));

                    assert_eq!(
                        responses["foo"],
                        vec![
                            ListedOffset {
                                partition_id: 0,
                                error_code: KafkaCode::None,
                                offsets: vec![0],
                                timestamp: None,
                            },
                        ]
                    );
                    assert_eq!(
                        responses["bar"],
                        (0..4)
                            .map(|partition_id| ListedOffset {
                                partition_id,
                                error_code: KafkaCode::None,
                                offsets: vec![0],
                                timestamp: None,
                            })
                            .collect::<Vec<_>>()
                    );
                })
            })
        }).unwrap()
    }
}

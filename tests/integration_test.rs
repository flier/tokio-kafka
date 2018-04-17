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

    const TOPIC_FOO_PARTITIONS: usize = 1;
    const TOPIC_FOO_MESSAGE_COUNT: i64 = 10;
    const TOPIC_BAR_PARTITIONS: usize = 4;
    const TOPIC_BAR_MESSAGE_COUNT: i64 = 10;

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

                    assert_eq!(topics["foo"].len(), TOPIC_FOO_PARTITIONS);
                    assert_eq!(topics["bar"].len(), TOPIC_BAR_PARTITIONS);

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
                                offsets: vec![TOPIC_FOO_MESSAGE_COUNT, 0],
                                timestamp: None,
                            },
                        ]
                    );

                    let offsets = &responses["bar"];

                    assert_eq!(offsets.len(), TOPIC_BAR_PARTITIONS);
                    assert_eq!(
                        offsets
                            .iter()
                            .map(|offset| offset.offsets.iter().cloned().max().unwrap_or_default())
                            .sum::<i64>(),
                        TOPIC_BAR_MESSAGE_COUNT
                    );
                })
            })
        }).unwrap()
    }
}

use std::borrow::Cow;
use std::collections::HashMap;

use serde::{de, ser};

use protocol::Nullable;
use consumer::{Assignment, Subscription};

const CONSUMER_PROTOCOL_V0: i16 = 0;

pub const CONSUMER_PROTOCOL: &str = "consumer";

pub struct ConsumerProtocol {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConsumerProtocolHeader {
    version: i16,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionSchema {
    header: ConsumerProtocolHeader,
    topics: Vec<String>,
    user_data: Nullable<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TopicAssignment {
    topics: String,
    partitions: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssignmentSchema {
    header: ConsumerProtocolHeader,
    topic_partitions: Vec<TopicAssignment>,
    user_data: Nullable<Vec<u8>>,
}

impl<'a> ser::Serialize for Subscription<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: ser::Serializer
    {
        let mut schema = SubscriptionSchema {
            header: ConsumerProtocolHeader { version: CONSUMER_PROTOCOL_V0 },
            topics: self.topics
                .iter()
                .map(|topic_name| String::from(topic_name.to_owned()))
                .collect(),
            user_data: self.user_data
                .as_ref()
                .map(|user_data| user_data.to_vec())
                .into(),
        };

        schema.topics.sort();

        schema.serialize(serializer)
    }
}

impl<'a, 'de> de::Deserialize<'de> for Subscription<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Subscription<'a>, D::Error>
        where D: de::Deserializer<'de>
    {
        let SubscriptionSchema {
            header,
            topics,
            user_data,
        } = SubscriptionSchema::deserialize(deserializer)?;

        if header.version < CONSUMER_PROTOCOL_V0 {
            Err(de::Error::custom(format!("unsupported subscription version: {}", header.version)))
        } else {
            Ok(Subscription {
                   topics: topics.into_iter().map(Cow::Owned).collect(),
                   user_data: user_data.into_raw().map(Cow::Owned),
               })
        }
    }
}

impl<'a> ser::Serialize for Assignment<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: ser::Serializer
    {
        let mut topic_partitions = HashMap::new();

        for tp in &self.partitions {
            topic_partitions
                .entry(tp.topic_name.to_owned())
                .or_insert_with(Vec::new)
                .push(tp.partition);
        }

        let mut schema = AssignmentSchema {
            header: ConsumerProtocolHeader { version: CONSUMER_PROTOCOL_V0 },
            topic_partitions: topic_partitions
                .into_iter()
                .map(|(topic_name, partitions)| {
                         TopicAssignment {
                             topics: String::from(topic_name.to_owned()),
                             partitions: partitions,
                         }
                     })
                .collect(),
            user_data: self.user_data
                .as_ref()
                .map(|user_data| user_data.to_vec())
                .into(),
        };

        schema
            .topic_partitions
            .sort_by(|lhs, rhs| lhs.topics.cmp(&rhs.topics));

        schema.serialize(serializer)
    }
}

impl<'a, 'de> de::Deserialize<'de> for Assignment<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Assignment<'a>, D::Error>
        where D: de::Deserializer<'de>
    {
        let AssignmentSchema {
            header,
            topic_partitions,
            user_data,
        } = AssignmentSchema::deserialize(deserializer)?;

        if header.version < CONSUMER_PROTOCOL_V0 {
            Err(de::Error::custom(format!("unsupported assignment version: {}", header.version)))
        } else {
            let partitions = topic_partitions
                .iter()
                .flat_map(|assignment| {
                    let topic_name = String::from(assignment.topics.to_owned());

                    assignment
                        .partitions
                        .iter()
                        .map(move |&partition| topic_partition!(topic_name.clone(), partition))
                })
                .collect();

            Ok(Assignment {
                   partitions: partitions,
                   user_data: user_data.into_raw().map(Cow::Owned),
               })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use protocol::Schema;

    lazy_static! {
        static ref TEST_SUBSCRIPTION: Subscription<'static> = Subscription {
            topics: vec!["t0".into(), "t1".into()],
            user_data: Some(b"data".to_vec().into()),
        };

        static ref TEST_SUBSCRIPTION_DATA: Vec<u8> = vec![
            // SubscriptionSchema
            // header: ConsumerProtocolHeader
                0, 0, // version

            // topic_partitions: [&str]
            0, 0, 0, 2,
                0, 2, b't', b'0',
                0, 2, b't', b'1',

            // user_data
            0, 0, 0, 4, b'd', b'a', b't', b'a',
        ];

        static ref TEST_ASSIGNMENT: Assignment<'static> = Assignment {
            partitions: vec![
                topic_partition!("t0", 0),
                topic_partition!("t0", 1),
                topic_partition!("t1", 0),
                topic_partition!("t1", 1)
            ],
            user_data: Some(b"data".to_vec().into()),
        };

        static ref TEST_ASSIGNMENT_DATA: Vec<u8> = vec![
            // AssignmentSchema
            // header: ConsumerProtocolHeader
                0, 0, // version

            // partitions: [TopicAssignment]
            0, 0, 0, 2,
                // TopicAssignment
                0, 2, b't', b'0',   // topics
                0, 0, 0, 2,         // partitions
                    0, 0, 0, 0,
                    0, 0, 0, 1,
                // TopicAssignment
                0, 2, b't', b'1',   // topics
                0, 0, 0, 2,         // partitions
                    0, 0, 0, 0,
                    0, 0, 0, 1,

            // user_data
            0, 0, 0, 4, b'd', b'a', b't', b'a',
        ];
    }

    #[test]
    fn test_subscription_serializer() {
        assert_eq!(Schema::serialize(&*TEST_SUBSCRIPTION).unwrap(),
                   *TEST_SUBSCRIPTION_DATA);
    }

    #[test]
    fn test_subscription_deserializer() {
        let subscription: Subscription =
            Schema::deserialize(Cursor::new(TEST_SUBSCRIPTION_DATA.clone())).unwrap();

        assert_eq!(subscription, *TEST_SUBSCRIPTION);
    }

    #[test]
    fn test_assignment_serializer() {
        assert_eq!(Schema::serialize(&*TEST_ASSIGNMENT).unwrap(),
                   *TEST_ASSIGNMENT_DATA);
    }

    #[test]
    fn test_assignment_deserializer() {
        let assignment: Assignment = Schema::deserialize(Cursor::new(TEST_ASSIGNMENT_DATA.clone()))
            .unwrap();

        assert_eq!(assignment, *TEST_ASSIGNMENT);
    }
}

use bytes::{ByteOrder, BytesMut};

use nom::{be_i16, be_i32};

use errors::Result;
use protocol::{Encodable, ErrorCode, NodeId, ParseTag, PartitionId, RequestHeader, ResponseHeader,
               WriteExt, parse_response_header, parse_string};

#[derive(Clone, Debug, PartialEq)]
pub struct MetadataRequest {
    pub header: RequestHeader,
    pub topic_names: Vec<String>,
}

impl Encodable for MetadataRequest {
    fn encode<T: ByteOrder>(self, dst: &mut BytesMut) -> Result<()> {
        self.header.encode::<T>(dst)?;

        dst.put_array::<T, _, _>(self.topic_names,
                                 |buf, topic_name| buf.put_str::<T, _>(Some(topic_name)))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetadataResponse {
    pub header: ResponseHeader,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BrokerMetadata {
    pub node_id: NodeId,
    pub host: String,
    pub port: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopicMetadata {
    pub error_code: ErrorCode,
    pub topic_name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PartitionMetadata {
    pub error_code: ErrorCode,
    pub partition: PartitionId,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
}

named!(pub parse_metadata_response<MetadataResponse>,
    parse_tag!(ParseTag::MetadataResponse,
        do_parse!(
            header: parse_response_header
        >> brokers: length_count!(be_i32, parse_broker_metadata)
        >> topics: length_count!(be_i32, parse_topic_metadata)
        >> (MetadataResponse {
                header: header,
                brokers: brokers,
                topics: topics,
            })
        )
    )
);

named!(parse_broker_metadata<BrokerMetadata>,
    parse_tag!(ParseTag::BrokerMetadata,
        do_parse!(
            node_id: be_i32
         >> host: parse_string
         >> port: be_i32
         >> (BrokerMetadata {
                node_id: node_id,
                host: host,
                port: port,
            })
        )
    )
);

named!(parse_topic_metadata<TopicMetadata>,
    parse_tag!(ParseTag::TopicMetadata,
        do_parse!(
            error_code: be_i16
         >> topic_name: parse_string
         >> partitions: length_count!(be_i32, parse_partition_metadata)
         >> (TopicMetadata {
                error_code: error_code,
                topic_name: topic_name,
                partitions: partitions,
            })
        )
    )
);

named!(parse_partition_metadata<PartitionMetadata>,
    parse_tag!(ParseTag::PartitionMetadata,
        do_parse!(
            error_code: be_i16
         >> partition: be_i32
         >> leader: be_i32
         >> replicas: length_count!(be_i32, be_i32)
         >> isr: length_count!(be_i32, be_i32)
         >> (PartitionMetadata {
                error_code: error_code,
                partition: partition,
                leader: leader,
                replicas: replicas,
                isr: isr,
            })
        )
    )
);

#[cfg(test)]
mod tests {
    use bytes::{BigEndian, BytesMut};

    use nom::IResult;

    use super::*;
    use protocol::*;

    lazy_static!{
        static ref TEST_REQUEST_DATA: Vec<u8> = vec![
            // ProduceRequest
                // RequestHeader
                0, 3,                               // api_key
                0, 0,                               // api_version
                0, 0, 0, 123,                       // correlation_id
                0, 6, 99, 108, 105, 101, 110, 116,  // client_id
            // topic_names: [String]
            0, 0, 0, 1,
                0, 5, b't', b'o', b'p', b'i', b'c',     // topic_name
        ];

        static ref TEST_RESPONSE_DATA: Vec<u8> = vec![
            // ResponseHeader
            0, 0, 0, 123, // correlation_id
            // brokers: [BrokerMetadata]
            0, 0, 0, 1,
                0, 0, 0, 1,                         // node_id
                0, 4, b'h', b'o', b's', b't',       // host
                0, 0, 0, 80,                        // port
            // topics: [TopicMetadata]
            0, 0, 0, 1,
                0, 2,                               // error_code
                0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                // partitions: [PartitionMetadata]
                0, 0, 0, 1,
                    0, 3,                           // error_code
                    0, 0, 0, 4,                     // partition_id
                    0, 0, 0, 5,                     // leader
                    // replicas: [ReplicaId]
                    0, 0, 0, 1,
                        0, 0, 0, 6,
                    // isr: [i32]
                    0, 0, 0, 1,
                        0, 0, 0, 7,
        ];

        static ref TEST_RESPONSE: MetadataResponse = MetadataResponse {
            header: ResponseHeader { correlation_id: 123 },
            brokers: vec![BrokerMetadata {
                node_id: 1,
                host: "host".to_owned(),
                port: 80,
            }],
            topics: vec![TopicMetadata {
                error_code: 2,
                topic_name: "topic".to_owned(),
                partitions: vec![PartitionMetadata {
                    error_code: 3,
                    partition: 4,
                    leader: 5,
                    replicas: vec![6],
                    isr: vec![7],
                }],
            }],
        };
    }

    #[test]
    fn test_encode_metadata_request() {
        let req = MetadataRequest {
            header: RequestHeader {
                api_key: ApiKeys::Metadata as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".to_owned()),
            },
            topic_names: vec!["topic".to_owned()],
        };

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(&buf[..], &TEST_REQUEST_DATA[..]);
    }

    #[test]
    fn test_parse_metadata_response() {
        assert_eq!(parse_metadata_response(TEST_RESPONSE_DATA.as_slice()),
        IResult::Done(&[][..], TEST_RESPONSE.clone()));
    }
}
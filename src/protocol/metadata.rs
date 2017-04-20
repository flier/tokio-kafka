use std::marker::PhantomData;

use bytes::{BytesMut, ByteOrder};

use nom::{be_i16, be_i32};

use errors::Result;
use codec::WriteExt;
use protocol::{RequestHeader, ResponseHeader, parse_response_header, parse_string};

#[derive(Clone, Debug, PartialEq)]
pub struct MetadataRequest {
    pub header: RequestHeader,
    pub topic_name: String,
}

pub struct MetadataRequestEncoder<T>(PhantomData<T>);

impl<T> MetadataRequestEncoder<T> {
    pub fn new() -> Self {
        MetadataRequestEncoder(PhantomData)
    }
}

impl<T: ByteOrder> MetadataRequestEncoder<T> {
    pub fn encode(&mut self, req: MetadataRequest, dst: &mut BytesMut) -> Result<()> {
        dst.put_item::<T, _>(req.header)?;
        dst.put_str::<T, _>(Some(req.topic_name))
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
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub topic_name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

named!(pub parse_metadata_response<MetadataResponse>,
    do_parse!(
        header: parse_response_header
     >> n: be_i32
     >> brokers: many_m_n!(n as usize, n as usize, parse_broker_metadata)
     >> n: be_i32
     >> topics: many_m_n!(n as usize, n as usize, parse_topic_metadata)
     >> (MetadataResponse {
            header: header,
            brokers: brokers,
            topics: topics,
        })
    )
);

named!(parse_broker_metadata<BrokerMetadata>,
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
);

named!(parse_topic_metadata<TopicMetadata>,
    do_parse!(
        error_code: be_i16
     >> topic_name: parse_string
     >> n: be_i32
     >> partitions: many_m_n!(n as usize, n as usize, parse_partition_metadata)
     >> (TopicMetadata {
            error_code: error_code,
            topic_name: topic_name,
            partitions: partitions,
        })
    )
);

named!(parse_partition_metadata<PartitionMetadata>,
    do_parse!(
        error_code: be_i16
     >> partition_id: be_i32
     >> leader: be_i32
     >> n: be_i32
     >> replicas: many_m_n!(n as usize, n as usize, be_i32)
     >> n: be_i32
     >> isr: many_m_n!(n as usize, n as usize, be_i32)
     >> (PartitionMetadata {
         error_code: error_code,
         partition_id: partition_id,
         leader: leader,
         replicas: replicas,
         isr: isr,
     })
    )
);

#[cfg(test)]
mod tests {
    use bytes::BigEndian;

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
                    // replicas: [i32]
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
                    partition_id: 4,
                    leader: 5,
                    replicas: vec![6],
                    isr: vec![7],
                }],
            }],
        };
    }

    #[test]
    fn test_metadata_request_encoder() {
        let req = MetadataRequest {
            header: RequestHeader {
                api_key: ApiKeys::Metadata as i16,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".to_owned()),
            },
            topic_name: "topic".to_owned(),
        };

        let mut encoder = MetadataRequestEncoder::<BigEndian>::new();

        let mut buf = BytesMut::with_capacity(128);

        encoder.encode(req, &mut buf).unwrap();

        assert_eq!(&buf[..], &TEST_REQUEST_DATA[..]);
    }

    #[test]
    fn test_metadata_request_decoder() {
        assert_eq!(parse_metadata_response(TEST_RESPONSE_DATA.as_slice()),
        IResult::Done(&[][..], TEST_RESPONSE.clone()));
    }
}
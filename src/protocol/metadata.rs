use std::result;
use std::marker::PhantomData;

use bytes::{BytesMut, ByteOrder};

use nom::{IResult, be_i16, be_i32};

use tokio_io::codec::{Encoder, Decoder};

use errors::{Error, Result};
use codec::WriteExt;
use protocol::{RequestHeader, ResponseHeader, parse_response_header, parse_string};

#[derive(Clone, Debug, PartialEq)]
pub struct TopicMetadataRequest<'a> {
    pub header: RequestHeader<'a>,
    pub topic_name: &'a str,
}

pub struct TopicMetadataRequestEncoder<'a, T: 'a>(PhantomData<&'a T>);

impl<'a, T: 'a> TopicMetadataRequestEncoder<'a, T> {
    pub fn new() -> Self {
        TopicMetadataRequestEncoder(PhantomData)
    }
}

impl<'a, T: 'a + ByteOrder> Encoder for TopicMetadataRequestEncoder<'a, T> {
    type Item = TopicMetadataRequest<'a>;
    type Error = Error;

    fn encode(&mut self, req: Self::Item, dst: &mut BytesMut) -> Result<()> {
        dst.put_item::<T, _>(&req.header)?;
        dst.put_str::<T, _>(req.topic_name)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopicMetadataResponse {
    pub header: ResponseHeader,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
}
/*
impl<'a> TopicMetadataResponse<'a> {
    pub fn parse<T: 'a + AsRef<[u8]>>(buf: T) -> Result<TopicMetadataResponse<'a>> {}
}
*/
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

pub struct TopicMetadataResponseDecoder;

impl TopicMetadataResponseDecoder {
    pub fn new() -> Self {
        TopicMetadataResponseDecoder {}
    }
}

impl Decoder for TopicMetadataResponseDecoder {
    type Item = TopicMetadataResponse;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> result::Result<Option<Self::Item>, Self::Error> {
        let (off, res) = match parse_topic_metadata_response(src.as_ref()) {
            IResult::Done(remaining, res) => {
                let len = src.len() - remaining.len();

                (Some(len), Ok(Some(res)))
            }
            IResult::Incomplete(_) => (None, Ok(None)),
            IResult::Error(err) => (None, Err(err.into())),
        };

        if let Some(off) = off {
            src.split_to(off);
        }

        res
    }
}

named!(parse_topic_metadata_response<TopicMetadataResponse>,
    do_parse!(
        header: parse_response_header
     >> n: be_i32
     >> brokers: many_m_n!(n as usize, n as usize, parse_broker_metadata)
     >> n: be_i32
     >> topics: many_m_n!(n as usize, n as usize, parse_topic_metadata)
     >> (TopicMetadataResponse {
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

        static ref TEST_RESPONSE: TopicMetadataResponse = TopicMetadataResponse {
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
    fn test_topic_metadata_request_encoder() {
        let req = TopicMetadataRequest {
            header: RequestHeader {
                api_key: ApiKeys::Metadata as i16,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client"),
            },
            topic_name: "topic",
        };

        let mut encoder = TopicMetadataRequestEncoder::<BigEndian>::new();

        let mut buf = BytesMut::with_capacity(128);

        encoder.encode(req, &mut buf).unwrap();

        assert_eq!(&buf[..], &TEST_REQUEST_DATA[..]);
    }

    #[test]
    fn test_topic_metadata_request_decoder() {
        let mut buf = BytesMut::from(TEST_RESPONSE_DATA.as_slice());
        let mut decoder = TopicMetadataResponseDecoder::new();

        assert_eq!(decoder.decode(&mut buf).unwrap().unwrap(), TEST_RESPONSE.clone());
        assert_eq!(buf.len(), 0);

        assert_eq!(decoder.decode(&mut buf).unwrap(), None);
    }
}
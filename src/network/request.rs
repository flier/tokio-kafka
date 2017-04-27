use std::io;

use bytes::{BytesMut, BigEndian};

use compression::Compression;
use protocol::{RequestHeader, ProduceRequest, ProduceRequestEncoder, MetadataRequest,
               MetadataRequestEncoder, ApiVersionsRequest, ApiVersionsRequestEncoder};

#[derive(Debug)]
pub enum KafkaRequest {
    Produce {
        req: ProduceRequest,
        compression: Compression,
    },
    Metadata(MetadataRequest),
    ApiVersions(ApiVersionsRequest),
}

impl KafkaRequest {
    pub fn encode(self, buf: &mut BytesMut) -> io::Result<()> {
        let res = match self {
            KafkaRequest::Produce { req, compression } => {
                ProduceRequestEncoder::new(compression).encode::<BigEndian>(req, buf)
            }
            KafkaRequest::Metadata(req) => {
                MetadataRequestEncoder::new().encode::<BigEndian>(req, buf)
            }
            KafkaRequest::ApiVersions(req) => {
                ApiVersionsRequestEncoder::new().encode::<BigEndian>(req, buf)
            }
        };

        res.map_err(|err| {
                        warn!("fail to encode request, {}", err);

                        io::Error::new(io::ErrorKind::InvalidInput, err.to_string())
                    })
    }

    pub fn header(&self) -> &RequestHeader {
        match self {
            &KafkaRequest::Produce { ref req, .. } => &req.header,
            &KafkaRequest::Metadata(ref req) => &req.header,
            &KafkaRequest::ApiVersions(ref req) => &req.header,
        }
    }
}
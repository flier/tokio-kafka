use std::io;

use bytes::{BytesMut, BigEndian};

use compression::Compression;
use protocol::{ProduceRequest, ProduceRequestEncoder, MetadataRequest, MetadataRequestEncoder};

pub enum KafkaRequest {
    Produce {
        req: ProduceRequest,
        api_version: i16,
        compression: Compression,
    },
    Fetch,
    Offsets,
    Metadata(MetadataRequest),
}

impl KafkaRequest {
    pub fn encode(self, buf: &mut BytesMut) -> io::Result<()> {
        let res = match self {
            KafkaRequest::Produce {
                req,
                api_version,
                compression,
            } => ProduceRequestEncoder::<BigEndian>::new(api_version, compression).encode(req, buf),
            KafkaRequest::Metadata(req) => {
                MetadataRequestEncoder::<BigEndian>::new().encode(req, buf)
            }
            _ => unimplemented!(),
        };

        res.map_err(|err| {
                        warn!("fail to encode request, {}", err);

                        io::Error::new(io::ErrorKind::InvalidInput, err.to_string())
                    })
    }
}
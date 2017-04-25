use std::io;

use bytes::{BytesMut, BigEndian};

use compression::Compression;
use protocol::{ProduceRequest, ProduceRequestEncoder, MetadataRequest, MetadataRequestEncoder};

#[derive(Debug)]
pub enum KafkaRequest {
    Produce {
        req: ProduceRequest,
        compression: Compression,
    },
    Fetch,
    Offsets,
    Metadata(MetadataRequest),
}

impl KafkaRequest {
    pub fn encode(self, buf: &mut BytesMut) -> io::Result<()> {
        let res = match self {
            KafkaRequest::Produce { req, compression } => {
                ProduceRequestEncoder::<BigEndian>::new(compression).encode(req, buf)
            }
            KafkaRequest::Metadata(req) => {
                MetadataRequestEncoder::<BigEndian>::new().encode(req, buf)
            }
            _ => unimplemented!(),
        };

        trace!("request encoded:\n{}", hexdump!(&buf[..]));

        res.map_err(|err| {
                        warn!("fail to encode request, {}", err);

                        io::Error::new(io::ErrorKind::InvalidInput, err.to_string())
                    })
    }
}
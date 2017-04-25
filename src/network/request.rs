use std::io;

use bytes::{BytesMut, BigEndian};

use compression::Compression;
use protocol::{RequestHeader, ProduceRequest, ProduceRequestEncoder, MetadataRequest,
               MetadataRequestEncoder};

#[derive(Debug)]
pub enum KafkaRequest {
    Produce {
        req: ProduceRequest,
        compression: Compression,
    },
    Metadata(MetadataRequest),
}

impl KafkaRequest {
    pub fn encode(self, buf: &mut BytesMut) -> io::Result<()> {
        let off = buf.len();
        let res = match self {
            KafkaRequest::Produce { req, compression } => {
                ProduceRequestEncoder::<BigEndian>::new(compression).encode(req, buf)
            }
            KafkaRequest::Metadata(req) => {
                MetadataRequestEncoder::<BigEndian>::new().encode(req, buf)
            }
        };

        trace!("request encoded as {} bytes:\n{}",
               buf.len() - off,
               hexdump!(&buf[off..], off));

        res.map_err(|err| {
                        warn!("fail to encode request, {}", err);

                        io::Error::new(io::ErrorKind::InvalidInput, err.to_string())
                    })
    }

    pub fn header(&self) -> &RequestHeader {
        match self {
            &KafkaRequest::Produce { ref req, .. } => &req.header,
            &KafkaRequest::Metadata(ref req) => &req.header,
        }
    }
}
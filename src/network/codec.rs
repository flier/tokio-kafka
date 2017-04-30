use std::io;
use std::mem;
use std::collections::VecDeque;

use bytes::{BufMut, BytesMut, ByteOrder, BigEndian};

use tokio_io::codec::{Encoder, Decoder};

use protocol::{Encodable, ApiKeys, ApiVersion, RequestHeader, CorrelationId};
use network::{KafkaRequest, KafkaResponse};

#[derive(Debug)]
pub struct KafkaCodec {
    requests: VecDeque<(ApiKeys, ApiVersion, CorrelationId)>,
}

impl KafkaCodec {
    pub fn new() -> Self {
        KafkaCodec { requests: VecDeque::new() }
    }
}

impl Encoder for KafkaCodec {
    type Item = KafkaRequest;
    type Error = io::Error;

    fn encode(&mut self, request: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let off = dst.len();

        dst.put_i32::<BigEndian>(0);

        let &RequestHeader {
                 api_key,
                 api_version,
                 correlation_id,
                 ..
             } = request.header();

        request
            .encode::<BigEndian>(dst)
            .map_err(|err| {
                         io::Error::new(io::ErrorKind::InvalidInput,
                                        format!("invalid request, {}", err))
                     })?;

        let size = dst.len() - off - mem::size_of::<i32>();

        BigEndian::write_i32(&mut dst[off..off + mem::size_of::<i32>()], size as i32);

        trace!("encoded {} bytes frame:\n{}",
               size + mem::size_of::<i32>(),
               hexdump!(&dst[..]));

        self.requests
            .push_back((ApiKeys::from(api_key), api_version, correlation_id));

        Ok(())
    }
}

impl Decoder for KafkaCodec {
    type Item = KafkaResponse;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let size_header_len = mem::size_of::<u32>();

        if src.len() < size_header_len {
            Ok(None)
        } else {
            let size = BigEndian::read_i32(&src[..]) as usize;

            if size_header_len + size > src.len() {
                Ok(None)
            } else {
                trace!("received {} bytes frame:\n{}",
                       src.len(),
                       hexdump!(&src[..]));

                let buf = src.split_to(size + size_header_len)
                    .split_off(size_header_len)
                    .freeze();

                if let Some((api_key, api_version, correlation_id)) = self.requests.pop_front() {
                    if BigEndian::read_i32(&buf[..]) != correlation_id {
                        Err(io::Error::new(io::ErrorKind::InvalidData, "correlation id mismatch"))
                    } else {
                        KafkaResponse::parse(&buf[..], api_key, api_version)
                    }
                } else {
                    Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected response"))
                }
            }
        }
    }
}
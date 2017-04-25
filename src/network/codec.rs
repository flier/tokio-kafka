use std::io;
use std::mem;

use bytes::{BufMut, BytesMut, ByteOrder, BigEndian};

use tokio_io::codec::{Encoder, Decoder};

use protocol::ApiVersion;
use network::{KafkaRequest, KafkaResponse};

#[derive(Debug)]
pub struct KafkaCodec {
    api_version: ApiVersion,
}

impl KafkaCodec {
    pub fn new(api_version: ApiVersion) -> Self {
        KafkaCodec { api_version: api_version }
    }
}

impl Encoder for KafkaCodec {
    type Item = KafkaRequest;
    type Error = io::Error;

    fn encode(&mut self, request: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let off = dst.len();

        dst.put_i32::<BigEndian>(0);

        request.encode(dst)?;

        let size = dst.len() - off - mem::size_of::<i32>();

        BigEndian::write_i32(&mut dst[off..off + mem::size_of::<i32>()], size as i32);

        trace!("frame encoded as {} bytes:\n{}",
               size + mem::size_of::<i32>(),
               hexdump!(&dst[..]));

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
                trace!("received {} bytes frame\n{}", src.len(), hexdump!(&src[..]));

                let buf = src.split_to(size + size_header_len)
                    .split_off(size_header_len)
                    .freeze();

                KafkaResponse::parse(&buf[..], self.api_version)
            }
        }
    }
}
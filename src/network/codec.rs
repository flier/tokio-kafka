use std::io;
use std::mem;

use bytes::{BytesMut, ByteOrder, BigEndian};

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

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst)
    }
}

impl Decoder for KafkaCodec {
    type Item = KafkaResponse;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < mem::size_of::<u32>() {
            Ok(None)
        } else {
            let size = BigEndian::read_i32(&src[..]) as usize;

            if mem::size_of::<u32>() + size > src.len() {
                Ok(None)
            } else {
                let buf = src.split_to(size)
                    .split_off(mem::size_of::<u32>())
                    .freeze();

                KafkaResponse::parse(&buf[..], self.api_version)
            }
        }
    }
}
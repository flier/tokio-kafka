use std::io;

use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_proto::pipeline::ClientProto;

use client::{KafkaRequest, KafkaResponse, KafkaCodec};

pub struct KafkaProto {
    api_version: i16,
}

impl KafkaProto {
    pub fn new(api_version: i16) -> Self {
        KafkaProto { api_version: api_version }
    }
}

impl<T: AsyncRead + AsyncWrite + 'static> ClientProto<T> for KafkaProto {
    type Request = KafkaRequest;
    type Response = KafkaResponse;
    type Transport = Framed<T, KafkaCodec>;
    type BindTransport = io::Result<Self::Transport>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(KafkaCodec::new(self.api_version)))
    }
}
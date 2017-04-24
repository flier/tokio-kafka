use std::io;

use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_proto::pipeline::ClientProto;

use protocol::ApiVersion;
use network::{KafkaRequest, KafkaResponse, KafkaCodec};

pub struct KafkaProto {
    api_version: ApiVersion,
}

impl KafkaProto {
    pub fn new(api_version: ApiVersion) -> Self {
        KafkaProto { api_version: api_version }
    }

    pub fn api_version(&self) -> ApiVersion {
        self.api_version
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
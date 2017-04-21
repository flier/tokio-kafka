use std::net::SocketAddr;

#[derive(Debug)]
pub struct KafkaConnection {
    addr: SocketAddr,
}

impl KafkaConnection {
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }
}
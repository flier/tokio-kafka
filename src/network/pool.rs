use std::fmt::Debug;
use std::net::SocketAddr;
use std::collections::HashMap;

use std::time::Instant;

use network::KafkaConnection;

#[derive(Debug)]
struct Pooled<T: Debug> {
    item: T,
    latest_checkpoint: Instant,
}

pub struct KafkaConnectionPool {
    conns: HashMap<SocketAddr, Pooled<KafkaConnection>>,
}
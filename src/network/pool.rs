use std::fmt::Debug;
use std::net::{SocketAddr, Shutdown};
use std::collections::HashMap;
use std::collections::vec_deque::VecDeque;

use std::time::{Instant, Duration};

use tokio_core::reactor::Handle;

use errors::Result;
use network::KafkaConnection;

#[derive(Debug)]
struct Pooled<T>
    where T: Debug
{
    item: T,
    ts: Instant,
}

impl<T: Debug> Pooled<T> {
    pub fn new(item: T) -> Self {
        Pooled {
            item: item,
            ts: Instant::now(),
        }
    }

    pub fn unwrap(self) -> T {
        self.item
    }
}

#[derive(Debug)]
struct State(u32);

impl State {
    fn new() -> State {
        State(0)
    }

    fn next_connection_id(&mut self) -> u32 {
        let id = self.0;
        self.0 = self.0.wrapping_add(1);
        id
    }
}

#[derive(Debug)]
struct Config {
    max_idle_timeout: Duration,
    max_pool_connections: usize,
}

#[derive(Debug)]
pub struct KafkaConnectionPool {
    conns: HashMap<SocketAddr, VecDeque<Pooled<KafkaConnection>>>,
    config: Config,
    state: State,
}

impl KafkaConnectionPool {
    pub fn new(max_idle_timeout: Duration, max_pool_connections: usize) -> Self {
        KafkaConnectionPool {
            conns: HashMap::new(),
            config: Config {
                max_idle_timeout: max_idle_timeout,
                max_pool_connections: max_pool_connections,
            },
            state: State::new(),
        }
    }

    pub fn get(&mut self, addr: &SocketAddr, handle: &Handle) -> Result<KafkaConnection> {
        if let Some(conns) = self.conns.get_mut(&addr) {
            while let Some(conn) = conns.pop_front() {
                if conn.ts.elapsed() >= self.config.max_idle_timeout {
                    debug!("drop timed out connection: {:?}", conn);
                } else {
                    return Ok(conn.unwrap());
                }
            }
        }

        Ok(KafkaConnection::tcp(self.state.next_connection_id(), &addr, &handle))
    }

    pub fn release(&mut self, addr: &SocketAddr, conn: KafkaConnection) {
        let conns = self.conns
            .entry(addr.clone())
            .or_insert_with(|| VecDeque::new());

        if conns.len() < self.config.max_pool_connections {
            conns.push_back(Pooled::new(conn));
        }
    }
}
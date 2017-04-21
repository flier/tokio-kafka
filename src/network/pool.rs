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

#[derive(Debug, Default)]
struct State(u32);

impl State {
    fn next_connection_id(&mut self) -> u32 {
        let id = self.0;
        self.0 = self.0.wrapping_add(1);
        id
    }
}

#[derive(Debug)]
struct Config {
    max_idle_timeout: Duration,
    max_pooled_connections: usize,
}

#[derive(Debug)]
pub struct KafkaConnectionPool {
    conns: HashMap<SocketAddr, VecDeque<Pooled<KafkaConnection>>>,
    config: Config,
    state: State,
}

impl KafkaConnectionPool {
    pub fn new(max_idle_timeout: Duration, max_pooled_connections: usize) -> Self {
        debug!("connection pool (max_idle={} seconds, max_pooled={})",
               max_idle_timeout.as_secs(),
               max_pooled_connections);

        KafkaConnectionPool {
            conns: HashMap::new(),
            config: Config {
                max_idle_timeout: max_idle_timeout,
                max_pooled_connections: max_pooled_connections,
            },
            state: State::default(),
        }
    }

    pub fn get(&mut self, addr: &SocketAddr, handle: &Handle) -> Result<KafkaConnection> {
        if let Some(conns) = self.conns.get_mut(&addr) {
            while let Some(conn) = conns.pop_front() {
                if conn.ts.elapsed() >= self.config.max_idle_timeout {
                    debug!("drop timed out connection: {:?}", conn);
                } else {
                    debug!("got pooled connection: {:?}", conn);

                    return Ok(conn.unwrap());
                }
            }
        }

        let conn = KafkaConnection::tcp(self.state.next_connection_id(), &addr, &handle);

        debug!("allocate new connection: {:?}", conn);

        Ok(conn)
    }

    pub fn release(&mut self, addr: &SocketAddr, conn: KafkaConnection) {
        let conns = self.conns
            .entry(addr.clone())
            .or_insert_with(|| VecDeque::new());

        if conns.len() < self.config.max_pooled_connections {
            debug!("release connection: {:?}", conn);

            conns.push_back(Pooled::new(conn));
        } else {
            debug!("drop overrun connection: {:?}", conn);
        }
    }
}
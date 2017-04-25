use std::io;
use std::fmt::Debug;
use std::net::{SocketAddr, ToSocketAddrs};

use futures_cpupool::{CpuPool, CpuFuture};

pub trait Resolver<S> {
    type Future;

    fn resolve(&mut self, addr: S) -> Self::Future;
}

pub struct DnsResolver {
    pool: CpuPool,
}

impl Default for DnsResolver {
    fn default() -> Self {
        DnsResolver { pool: CpuPool::new_num_cpus() }
    }
}

pub type DnsQuery = CpuFuture<Vec<SocketAddr>, io::Error>;

impl<S> Resolver<S> for DnsResolver
    where S: ToSocketAddrs + Send + Debug + 'static
{
    type Future = DnsQuery;

    fn resolve(&mut self, addr: S) -> Self::Future {
        self.pool
            .spawn_fn(move || {
                          let addrs = addr.to_socket_addrs().map(|it| it.collect());

                          trace!("resolve {:?} to {:?}", addr, addrs);

                          addrs
                      })
    }
}
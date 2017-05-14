use std::io;
use std::fmt::{self, Debug};
use std::net::{SocketAddr, ToSocketAddrs};

use futures::{Async, Future, Poll};
use futures_cpupool::{CpuFuture, CpuPool};

pub trait Resolver {
    type Future;

    fn resolve<S>(&self, addr: S) -> Self::Future where S: ToSocketAddrs + Debug + Send + 'static;
}

pub struct DnsResolver {
    pool: CpuPool,
}

impl Default for DnsResolver {
    fn default() -> Self {
        DnsResolver { pool: CpuPool::new_num_cpus() }
    }
}

impl Resolver for DnsResolver {
    type Future = DnsQuery;

    fn resolve<S>(&self, addr: S) -> Self::Future
        where S: ToSocketAddrs + Debug + Send + 'static
    {
        DnsQuery {
            state: State::Resolving(self.pool
                                        .spawn_fn(move || {
                let addrs = addr.to_socket_addrs().map(|it| it.collect());

                trace!("{:?} resolved to address: {:?}", addr, addrs);

                addrs
            })),
        }
    }
}

enum State {
    Resolving(CpuFuture<Vec<SocketAddr>, io::Error>),
    Resolved(Vec<SocketAddr>),
}

impl fmt::Debug for State {
    fn fmt(&self, w: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            State::Resolving(_) => write!(w, "resolving"),
            State::Resolved(ref addrs) => write!(w, "resolved({:?})", addrs),
        }
    }
}

#[derive(Debug)]
pub struct DnsQuery {
    state: State,
}

impl Future for DnsQuery {
    type Item = Vec<SocketAddr>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let state;

            match self.state {
                State::Resolving(ref mut query) => {
                    match query.poll() {
                        Ok(Async::Ready(addrs)) => state = State::Resolved(addrs),
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(err) => return Err(err),
                    }
                }
                State::Resolved(ref addrs) => return Ok(Async::Ready(addrs.clone())),
            }

            self.state = state;
        }
    }
}

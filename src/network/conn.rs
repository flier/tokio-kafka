use std::fmt;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

use futures::future::{Future, BoxFuture};
use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
use tokio_tls::{TlsConnectorExt, TlsStream};
use native_tls::TlsConnector;

use errors::Error;

#[derive(Debug)]
pub struct KafkaConnection {
    id: u32,
    stream: KafkaStream,
}

impl Deref for KafkaConnection {
    type Target = KafkaStream;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl DerefMut for KafkaConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

impl KafkaConnection {
    pub fn tcp(id: u32, addr: &SocketAddr, handle: &Handle) -> Self {
        KafkaConnection {
            id: id,
            stream: KafkaStream::tcp(addr.clone(),
                                     TcpStream::connect(addr, handle)
                                         .map_err(|err| err.into())
                                         .boxed()),
        }
    }

    pub fn tls(id: u32,
               addr: &SocketAddr,
               handle: &Handle,
               connector: TlsConnector,
               domain: &str)
               -> Self {
        let domain = domain.to_owned();

        KafkaConnection {
            id: id,
            stream: KafkaStream::tls(addr.clone(),
                                     TcpStream::connect(addr, handle)
                                         .map_err(|err| err.into())
                                         .and_then(move |sock| {
                                                       connector
                                                           .connect_async(&domain, sock)
                                                           .map_err(|err| err.into())
                                                   })
                                         .boxed()),
        }
    }
}

pub enum KafkaStream {
    Tcp {
        addr: SocketAddr,
        future: BoxFuture<TcpStream, Error>,
    },
    Tls {
        addr: SocketAddr,
        future: BoxFuture<TlsStream<TcpStream>, Error>,
    },
}

impl fmt::Debug for KafkaStream {
    fn fmt(&self, w: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &KafkaStream::Tcp { ref addr, .. } => write!(w, "TcpStream({})", addr),
            &KafkaStream::Tls { ref addr, .. } => write!(w, "TlsStream({})", addr),
        }
    }
}

impl KafkaStream {
    pub fn tcp(addr: SocketAddr, future: BoxFuture<TcpStream, Error>) -> Self {
        KafkaStream::Tcp {
            addr: addr,
            future: future,
        }
    }

    pub fn tls(addr: SocketAddr, future: BoxFuture<TlsStream<TcpStream>, Error>) -> Self {
        KafkaStream::Tls {
            addr: addr,
            future: future,
        }
    }

    pub fn addr(&self) -> &SocketAddr {
        match self {
            &KafkaStream::Tcp { ref addr, .. } |
            &KafkaStream::Tls { ref addr, .. } => addr,
        }
    }

    pub fn close(self) -> Self {
        match self {
            KafkaStream::Tcp { addr, future } => {
                KafkaStream::Tcp {
                    addr: addr,
                    future: future.fuse().boxed(),
                }
            }
            KafkaStream::Tls { addr, future } => {
                KafkaStream::Tls {
                    addr: addr,
                    future: future.fuse().boxed(),
                }
            }
        }
    }
}
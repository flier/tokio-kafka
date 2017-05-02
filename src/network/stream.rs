use std::fmt;
use std::io;
use std::io::prelude::*;
use std::net::{SocketAddr, ToSocketAddrs};

use futures::{Async, Poll};
use futures::future::Future;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_core::reactor::Handle;
use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_tls::{ConnectAsync, TlsConnectorExt, TlsStream};
use native_tls::TlsConnector;

use network::{DnsQuery, DnsResolver, Resolver};

pub struct KafkaConnector {
    handle: Handle,
    resolver: DnsResolver,
}

impl KafkaConnector {
    pub fn new(handle: Handle) -> Self {
        KafkaConnector {
            handle: handle,
            resolver: DnsResolver::default(),
        }
    }

    pub fn tcp<S>(&self, addr: S) -> Connect
        where S: ToSocketAddrs + fmt::Debug + Send + 'static
    {
        trace!("TCP connect to {:?}", addr);

        Connect {
            domain: None,
            connector: None,
            handle: self.handle.clone(),
            state: State::Resolving(self.resolver.resolve(addr)),
        }
    }

    pub fn tls<S>(&self, addr: S, connector: TlsConnector, domain: &str) -> Connect
        where S: ToSocketAddrs + fmt::Debug + Send + 'static
    {
        trace!("TLS connect to {:?}", addr);

        Connect {
            domain: Some(domain.to_owned()),
            connector: Some(connector),
            handle: self.handle.clone(),
            state: State::Resolving(self.resolver.resolve(addr)),
        }
    }
}

enum State {
    Resolving(DnsQuery),
    Connecting(TcpStreamNew, SocketAddr, Vec<SocketAddr>),
    Handshaking(ConnectAsync<TcpStream>, SocketAddr),
}

pub struct Connect {
    domain: Option<String>,
    connector: Option<TlsConnector>,
    handle: Handle,
    state: State,
}

impl Future for Connect {
    type Item = KafkaStream;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let state;

            let ref domain = self.domain;
            let ref connector = self.connector;

            match self.state {
                State::Resolving(ref mut future) => {
                    match future.poll() {
                        Ok(Async::Ready(mut addrs)) => {
                            addrs.reverse();

                            if let Some(addr) = addrs.pop() {
                                trace!("TCP connecting to {}", addr);

                                state = State::Connecting(TcpStream::connect(&addr, &self.handle),
                                                          addr,
                                                          addrs)
                            } else {
                                return Err(io::Error::new(io::ErrorKind::AddrNotAvailable,
                                                          "no more address"));
                            }
                        }
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(err) => {
                            warn!("fail to resolve address, {}", err);

                            return Err(err);
                        }
                    }
                }
                State::Connecting(ref mut future, addr, ref mut addrs) => {
                    match future.poll() {
                        Ok(Async::Ready(stream)) => {
                            if let (&Some(ref domain), &Some(ref connector)) = (domain, connector) {
                                trace!("TCP connected to {}, start TLS handshake", addr);

                                state = State::Handshaking(connector.connect_async(&domain,
                                                                                   stream),
                                                           addr);
                            } else {
                                trace!("TCP connected to {}", addr);

                                return Ok(Async::Ready(KafkaStream::Tcp(addr, stream)));
                            }
                        }
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(err) => {
                            warn!("fail to connect {}, {}", addr, err);

                            if let Some(addr) = addrs.pop() {
                                trace!("TCP connecting to {}", addr);

                                state = State::Connecting(TcpStream::connect(&addr, &self.handle),
                                                          addr,
                                                          addrs.clone())
                            } else {
                                return Err(io::Error::new(io::ErrorKind::AddrNotAvailable, err));
                            }
                        }
                    }
                }
                State::Handshaking(ref mut future, addr) => {
                    match future.poll() {
                        Ok(Async::Ready(stream)) => {
                            trace!("TLS connected to {}", addr);

                            return Ok(Async::Ready(KafkaStream::Tls(addr, stream)));
                        }
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(err) => {
                            warn!("fail to do TLS handshake to {}, {}", addr, err);

                            return Err(io::Error::new(io::ErrorKind::ConnectionAborted,
                                                      "TLS handshake failed"));
                        }
                    }
                }
            }

            self.state = state;
        }
    }
}

pub enum KafkaStream {
    Tcp(SocketAddr, TcpStream),
    Tls(SocketAddr, TlsStream<TcpStream>),
}

impl fmt::Debug for KafkaStream {
    fn fmt(&self, w: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &KafkaStream::Tcp(ref addr, _) => write!(w, "TcpStream({})", addr),
            &KafkaStream::Tls(ref addr, _) => write!(w, "TlsStream({})", addr),
        }
    }
}

impl Read for KafkaStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            &mut KafkaStream::Tcp(_, ref mut stream) => stream.read(buf),
            &mut KafkaStream::Tls(_, ref mut stream) => stream.read(buf),
        }
    }
}

impl Write for KafkaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            &mut KafkaStream::Tcp(_, ref mut stream) => stream.write(buf),
            &mut KafkaStream::Tls(_, ref mut stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            &mut KafkaStream::Tcp(_, ref mut stream) => stream.flush(),
            &mut KafkaStream::Tls(_, ref mut stream) => stream.flush(),
        }
    }
}

impl AsyncRead for KafkaStream {}

impl AsyncWrite for KafkaStream {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match self {
            &mut KafkaStream::Tcp(_, ref mut stream) => AsyncWrite::shutdown(stream),
            &mut KafkaStream::Tls(_, ref mut stream) => stream.shutdown(),
        }
    }
}

impl KafkaStream {
    pub fn addr(&self) -> &SocketAddr {
        match self {
            &KafkaStream::Tcp(ref addr, _) |
            &KafkaStream::Tls(ref addr, _) => addr,
        }
    }
}
use std::fmt;
use std::io;
use std::rc::Rc;
use std::io::prelude::*;
use std::net::SocketAddr;

use futures::future::Future;
use futures::{Async, Poll};
use native_tls::TlsConnector;
use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_core::reactor::Handle;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_tls::{ConnectAsync, TlsConnectorExt, TlsStream};
use ns_router::{AutoName, Router};
use ns_router::future::ResolveFuture;

use network::DEFAULT_PORT;

pub struct KafkaConnector {
    handle: Handle,
    router: Rc<Router>,
}

impl KafkaConnector {
    pub fn new(handle: Handle, router: Rc<Router>) -> Self {
        KafkaConnector { handle, router }
    }

    pub fn tcp<'n, N>(&self, addr: N) -> Connect
    where
        N: Into<AutoName<'n>> + fmt::Debug,
    {
        trace!("TCP connect to {:?}", addr);

        Connect {
            handle: self.handle.clone(),
            domain: None,
            connector: None,
            state: State::Resolving(self.router.resolve_auto(addr, DEFAULT_PORT)),
        }
    }

    pub fn tls<'n, N, S>(&self, addr: N, connector: TlsConnector, domain: S) -> Connect
    where
        N: Into<AutoName<'n>> + fmt::Debug,
        S: Into<String>,
    {
        trace!("TLS connect to {:?}", addr);

        Connect {
            handle: self.handle.clone(),
            domain: Some(domain.into()),
            connector: Some(connector),
            state: State::Resolving(self.router.resolve_auto(addr, DEFAULT_PORT)),
        }
    }
}

enum State {
    Resolving(ResolveFuture),
    Connecting(TcpStreamNew, SocketAddr, Vec<SocketAddr>),
    Handshaking(ConnectAsync<TcpStream>, SocketAddr),
}

pub struct Connect {
    handle: Handle,
    domain: Option<String>,
    connector: Option<TlsConnector>,
    state: State,
}

impl Future for Connect {
    type Item = KafkaStream;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let domain = &self.domain;
            let connector = &self.connector;

            let state = match self.state {
                State::Resolving(ref mut resolving) => match resolving.poll() {
                    Ok(Async::Ready(mut address)) => {
                        let mut addrs = address
                            .iter()
                            .flat_map(|weighted_set| weighted_set.addresses().collect::<Vec<_>>())
                            .collect::<Vec<_>>();

                        addrs.reverse();

                        if let Some(addr) = addrs.pop() {
                            trace!("TCP connecting to {}", addr);

                            State::Connecting(TcpStream::connect(&addr, &self.handle), addr, addrs)
                        } else {
                            bail!(io::Error::new(io::ErrorKind::AddrNotAvailable, "no more address"));
                        }
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(err) => {
                        bail!(io::Error::new(io::ErrorKind::AddrNotAvailable, err));
                    }
                },
                State::Connecting(ref mut connecting, peer_addr, ref mut addrs) => match connecting.poll() {
                    Ok(Async::Ready(stream)) => {
                        if let (&Some(ref domain), &Some(ref connector)) = (domain, connector) {
                            trace!("TCP connected to {}, start TLS handshake", peer_addr);

                            State::Handshaking(connector.connect_async(domain, stream), peer_addr)
                        } else {
                            trace!("TCP connected to {}", peer_addr);

                            return Ok(Async::Ready(KafkaStream::Tcp(peer_addr, stream)));
                        }
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(err) => {
                        warn!("fail to connect {}, {}", peer_addr, err);

                        if let Some(addr) = addrs.pop() {
                            trace!("TCP connecting to {}", addr);

                            State::Connecting(TcpStream::connect(&addr, &self.handle), addr, addrs.clone())
                        } else {
                            bail!(io::Error::new(io::ErrorKind::NotConnected, err));
                        }
                    }
                },
                State::Handshaking(ref mut handshaking, peer_addr) => match handshaking.poll() {
                    Ok(Async::Ready(stream)) => {
                        trace!("TLS connected to {}", peer_addr);

                        return Ok(Async::Ready(KafkaStream::Tls(peer_addr, stream)));
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(err) => {
                        warn!("fail to do TLS handshake to {}, {}", peer_addr, err);

                        bail!(io::Error::new(io::ErrorKind::ConnectionAborted, "TLS handshake failed"));
                    }
                },
            };

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
        match *self {
            KafkaStream::Tcp(ref addr, _) => write!(w, "TcpStream({})", addr),
            KafkaStream::Tls(ref addr, _) => write!(w, "TlsStream({})", addr),
        }
    }
}

impl Read for KafkaStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            KafkaStream::Tcp(_, ref mut stream) => stream.read(buf),
            KafkaStream::Tls(_, ref mut stream) => stream.read(buf),
        }
    }
}

impl Write for KafkaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            KafkaStream::Tcp(_, ref mut stream) => stream.write(buf),
            KafkaStream::Tls(_, ref mut stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            KafkaStream::Tcp(_, ref mut stream) => stream.flush(),
            KafkaStream::Tls(_, ref mut stream) => stream.flush(),
        }
    }
}

impl AsyncRead for KafkaStream {}

impl AsyncWrite for KafkaStream {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match *self {
            KafkaStream::Tcp(_, ref mut stream) => AsyncWrite::shutdown(stream),
            KafkaStream::Tls(_, ref mut stream) => stream.shutdown(),
        }
    }
}

impl KafkaStream {
    pub fn addr(&self) -> &SocketAddr {
        match *self {
            KafkaStream::Tcp(ref addr, _) | KafkaStream::Tls(ref addr, _) => addr,
        }
    }
}

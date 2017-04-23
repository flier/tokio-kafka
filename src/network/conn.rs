use std::fmt;
use std::io;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

use futures::{Async, Poll};
use futures::future::{Future, BoxFuture};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_core::reactor::Handle;
use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_tls::{TlsConnectorExt, TlsStream, ConnectAsync};
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

impl Read for KafkaConnection {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}

impl Write for KafkaConnection {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl AsyncRead for KafkaConnection {}

impl AsyncWrite for KafkaConnection {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.stream.shutdown()
    }
}

impl KafkaConnection {
    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn stream(&mut self) -> &mut KafkaStream {
        &mut self.stream
    }

    pub fn tcp(id: u32, addr: SocketAddr, handle: &Handle) -> Connect {
        debug!("TCP connecting to {}, id={}", addr, id);

        Connect::tcp(id, addr, handle)
    }

    pub fn tls(id: u32,
               addr: SocketAddr,
               handle: &Handle,
               connector: TlsConnector,
               domain: &str)
               -> Connect {
        debug!("TLS connecting to {} @ {}, id={}", domain, addr, id);

        Connect::tls(id, addr, domain, connector, handle)
    }
}

enum State {
    Connecting(TcpStreamNew),
    Handshaking(ConnectAsync<TcpStream>),
    Connected(KafkaStream),
}

pub struct Connect {
    id: u32,
    addr: SocketAddr,
    domain: Option<String>,
    connector: Option<TlsConnector>,
    state: State,
}

impl Connect {
    pub fn tcp(id: u32, addr: SocketAddr, handle: &Handle) -> Self {
        let state = State::Connecting(TcpStream::connect(&addr, handle));

        Connect {
            id: id,
            addr: addr,
            domain: None,
            connector: None,
            state: state,
        }
    }

    pub fn tls(id: u32,
               addr: SocketAddr,
               domain: &str,
               connector: TlsConnector,
               handle: &Handle)
               -> Self {
        let state = State::Connecting(TcpStream::connect(&addr, handle));

        Connect {
            id: id,
            addr: addr,
            domain: Some(domain.to_owned()),
            connector: Some(connector),
            state: state,
        }
    }
}

impl Future for Connect {
    type Item = KafkaConnection;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let state;

            match self.state {
                State::Connecting(ref mut future) => {
                    match future.poll() {
                        Ok(Async::Ready(stream)) => {
                            if let (Some(domain), Some(connector)) = (self.domain, self.connector) {
                                debug!("TCP connected to {}, id={}, start TLS handshake",
                                       self.addr,
                                       self.id);

                                state = State::Handshaking(connector.connect_async(&domain,
                                                                                   stream));
                            } else {
                                return Ok(Async::Ready(KafkaConnection {
                                                           id: self.id,
                                                           stream: KafkaStream::Tcp(self.addr,
                                                                                    stream),
                                                       }));
                            }
                        }
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(err) => {
                            warn!("fail to connect {}, id={}, {}", self.addr, self.id, err);

                            return bail!(err);
                        }
                    }
                }
                State::Handshaking(ref mut future) => {
                    return match future.poll() {
                               Ok(Async::Ready(stream)) => {
                        debug!("TLS connected to {}, id={}", self.addr, self.id);

                        Ok(Async::Ready(KafkaConnection {
                                            id: self.id,
                                            stream: KafkaStream::Tls(self.addr, stream),
                                        }))
                    }
                               Ok(Async::NotReady) => Ok(Async::NotReady),
                               Err(err) => {
                        warn!("fail to do TLS handshake to {}, id={}, {}",
                              self.addr,
                              self.id,
                              err);

                        bail!(err)
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
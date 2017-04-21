use std::fmt;
use std::io;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

use futures::Poll;
use futures::future::{Future, BoxFuture};
use tokio_io::{AsyncRead, AsyncWrite};
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

    pub fn tcp(id: u32, addr: SocketAddr, handle: &Handle) -> BoxFuture<Self, Error> {
        debug!("TCP connecting to {}, id={}", addr, id);

        TcpStream::connect(&addr, &handle)
            .map_err(move |err| {
                         warn!("fail to connect {}, id={}, {}", addr, id, err);

                         err.into()
                     })
            .map(move |stream| {
                     debug!("TCP connected to {}, id={}", addr, id);

                     KafkaConnection {
                         id: id,
                         stream: KafkaStream::Tcp(addr, stream),
                     }
                 })
            .boxed()
    }

    pub fn tls(id: u32,
               addr: SocketAddr,
               handle: &Handle,
               connector: TlsConnector,
               domain: &str)
               -> BoxFuture<Self, Error> {
        debug!("TLS connecting to {} @ {}, id={}", domain, addr, id);

        let domain = domain.to_owned();

        TcpStream::connect(&addr, &handle)
            .map_err(move |err| {
                         warn!("fail to connect {}, id={}, {}", addr, id, err);

                         err.into()
                     })
            .and_then(move |stream| {
                debug!("TCP connected to {}, id={}, start TLS handshake", addr, id);

                connector
                    .connect_async(&domain, stream)
                    .map_err(move |err| {
                                 warn!("fail to do TLS handshake to {}, id={}, {}", addr, id, err);

                                 err.into()
                             })
            })
            .map(move |stream| {
                     debug!("TLS connected to {}, id={}", addr, id);

                     KafkaConnection {
                         id: id,
                         stream: KafkaStream::Tls(addr, stream),
                     }
                 })
            .boxed()
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
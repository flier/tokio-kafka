use std::fmt;
use std::io;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

use bytes::BytesMut;

use futures::{Async, AsyncSink, Poll, StartSend};
use futures::stream::Stream;
use futures::sink::Sink;
use futures::future::Future;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_core::reactor::Handle;
use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_proto::streaming::pipeline::{Frame, Transport};
use tokio_tls::{TlsConnectorExt, TlsStream, ConnectAsync};
use native_tls::TlsConnector;

use network::{KafkaRequest, KafkaResponse};

#[derive(Debug)]
pub struct KafkaConnection<I> {
    id: u32,
    stream: I,
}

impl<I> Deref for KafkaConnection<I> {
    type Target = I;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl<I> DerefMut for KafkaConnection<I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

impl<I> Read for KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}

impl<I> Write for KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl<I> AsyncRead for KafkaConnection<I> where I: AsyncRead + AsyncWrite {}

impl<I> AsyncWrite for KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.stream.shutdown()
    }
}

impl<I> Stream for KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    type Item = Frame<KafkaResponse, BytesMut, io::Error>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::NotReady)
    }
}

impl<I> Sink for KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    type SinkItem = Frame<KafkaRequest, BytesMut, io::Error>;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        Ok(AsyncSink::Ready)
    }
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::NotReady)
    }
}

impl<I> Transport for KafkaConnection<I> where I: AsyncRead + AsyncWrite + 'static {}

impl<I> KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    pub fn new(id: u32, stream: I) -> Self {
        KafkaConnection {
            id: id,
            stream: stream,
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn stream(&mut self) -> &mut I {
        &mut self.stream
    }
}

#[derive(Clone, Debug)]
pub struct KafkaConnector {
    handle: Handle,
}

impl KafkaConnector {
    pub fn new(handle: &Handle) -> Self {
        KafkaConnector { handle: handle.clone() }
    }

    pub fn tcp(&mut self, addr: SocketAddr) -> Connect {
        debug!("TCP connecting to {}", addr);

        Connect::tcp(addr, &self.handle)
    }

    pub fn tls(&mut self, addr: SocketAddr, connector: TlsConnector, domain: &str) -> Connect {
        debug!("TLS connecting to {} @ {}", domain, addr);

        Connect::tls(addr, domain, connector, &self.handle)
    }
}

enum State {
    Connecting(TcpStreamNew),
    Handshaking(ConnectAsync<TcpStream>),
}

pub struct Connect {
    addr: SocketAddr,
    domain: Option<String>,
    connector: Option<TlsConnector>,
    state: State,
}

impl Connect {
    pub fn tcp(addr: SocketAddr, handle: &Handle) -> Self {
        let state = State::Connecting(TcpStream::connect(&addr, handle));

        Connect {
            addr: addr,
            domain: None,
            connector: None,
            state: state,
        }
    }

    pub fn tls(addr: SocketAddr, domain: &str, connector: TlsConnector, handle: &Handle) -> Self {
        let state = State::Connecting(TcpStream::connect(&addr, handle));

        Connect {
            addr: addr,
            domain: Some(domain.to_owned()),
            connector: Some(connector),
            state: state,
        }
    }
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
                State::Connecting(ref mut future) => {
                    match future.poll() {
                        Ok(Async::Ready(stream)) => {
                            if let (&Some(ref domain), &Some(ref connector)) = (domain, connector) {
                                debug!("TCP connected to {}, start TLS handshake", self.addr);

                                state = State::Handshaking(connector.connect_async(&domain,
                                                                                   stream));
                            } else {
                                return Ok(Async::Ready(KafkaStream::Tcp(self.addr, stream)));
                            }
                        }
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(err) => {
                            warn!("fail to connect {}, {}", self.addr, err);

                            return Err(err);
                        }
                    }
                }
                State::Handshaking(ref mut future) => {
                    return match future.poll() {
                               Ok(Async::Ready(stream)) => {
                        debug!("TLS connected to {}", self.addr);

                        Ok(Async::Ready(KafkaStream::Tls(self.addr, stream)))
                    }
                               Ok(Async::NotReady) => Ok(Async::NotReady),
                               Err(err) => {
                        warn!("fail to do TLS handshake to {}, {}", self.addr, err);

                        Err(io::Error::new(io::ErrorKind::ConnectionAborted,
                                           "TLS handshake failed"))
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
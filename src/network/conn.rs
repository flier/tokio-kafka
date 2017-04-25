use std::fmt;
use std::io;
use std::io::prelude::*;
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::{Deref, DerefMut};

use bytes::BytesMut;

use futures::{Async, AsyncSink, Poll, StartSend};
use futures::stream::Stream;
use futures::sink::Sink;
use futures::future::Future;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_core::reactor::Handle;
use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_proto::streaming::pipeline::{Frame, Transport};
use tokio_tls::{TlsConnectorExt, TlsStream, ConnectAsync};
use native_tls::TlsConnector;

use network::{KafkaRequest, KafkaResponse, KafkaCodec, Resolver, DnsResolver, DnsQuery};

#[derive(Debug)]
pub struct KafkaConnection<I> {
    id: u32,
    stream: Framed<I, KafkaCodec>,
}

impl<I> Deref for KafkaConnection<I> {
    type Target = I;

    fn deref(&self) -> &Self::Target {
        self.stream.get_ref()
    }
}

impl<I> DerefMut for KafkaConnection<I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.stream.get_mut()
    }
}

impl<I> Read for KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.stream.get_mut().read(buf) {
            Ok(size) => {
                trace!("read {} bytes:\n{}", size, hexdump!(&buf[..size]));

                Ok(size)
            }
            Err(err) => {
                trace!("read failed, {}", err);

                Err(err)
            }
        }
    }
}

impl<I> Write for KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!("write {} bytes:\n{}", buf.len(), hexdump!(buf));

        self.stream.get_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        trace!("flush stream");

        self.stream.get_mut().flush()
    }
}

impl<I> AsyncRead for KafkaConnection<I> where I: AsyncRead + AsyncWrite {}

impl<I> AsyncWrite for KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        trace!("shutdown stream");

        self.stream.get_mut().shutdown()
    }
}

impl<I> Stream for KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    type Item = Frame<KafkaResponse, BytesMut, io::Error>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.stream
            .poll()
            .map(|res| {
                res.map(|res| {
                    res.map(|res| {
                                trace!("received response: {:?}", res);

                                Frame::Message {
                                    message: res,
                                    body: false,
                                }
                            })

                })
            })
    }
}

impl<I> Sink for KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    type SinkItem = Frame<KafkaRequest, BytesMut, io::Error>;
    type SinkError = io::Error;

    fn start_send(&mut self, frame: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        trace!("send request: {:?}", frame);

        match frame {
            Frame::Message {
                message: request,
                body,
            } => {
                self.stream
                    .start_send(request)
                    .map(|async| match async {
                             AsyncSink::Ready => AsyncSink::Ready,
                             AsyncSink::NotReady(request) => {
                                 AsyncSink::NotReady(Frame::Message {
                                                         message: request,
                                                         body: body,
                                                     })
                             }
                         })
            }
            Frame::Body { .. } => Ok(AsyncSink::Ready),
            Frame::Error { .. } => Ok(AsyncSink::Ready),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        trace!("poll complete");

        self.stream.poll_complete()
    }
}

impl<I> Transport for KafkaConnection<I> where I: AsyncRead + AsyncWrite + 'static {}

impl<I> KafkaConnection<I>
    where I: AsyncRead + AsyncWrite
{
    pub fn new(id: u32, stream: I, codec: KafkaCodec) -> Self {
        KafkaConnection {
            id: id,
            stream: stream.framed(codec),
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }
}

pub struct KafkaConnector {
    handle: Handle,
    resolver: DnsResolver,
}

impl KafkaConnector {
    pub fn new(handle: &Handle) -> Self {
        KafkaConnector {
            handle: handle.clone(),
            resolver: DnsResolver::default(),
        }
    }

    pub fn tcp<S>(&mut self, addr: S) -> Connect
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

    pub fn tls<S>(&mut self, addr: S, connector: TlsConnector, domain: &str) -> Connect
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
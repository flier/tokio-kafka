use std::io;
use std::io::prelude::*;
use std::ops::{Deref, DerefMut};
use std::time::Instant;

use bytes::BytesMut;

use futures::sink::Sink;
use futures::stream::Stream;
use futures::{AsyncSink, Poll, StartSend};
use tokio_io::codec::Framed;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_proto::streaming::pipeline::{Frame, Transport};

use network::{ConnectionId, KafkaCodec, KafkaRequest, KafkaResponse};

#[derive(Clone, Copy, Debug)]
pub enum Status {
    Idle(Instant),
    Busy,
    Closed,
}

pub trait KeepAlive {
    fn status(&self) -> Status;
    fn busy(&mut self);
    fn close(&mut self);
    fn idle(&mut self);
}

#[derive(Debug)]
struct State<K> {
    keep_alive: K,
}

#[derive(Debug)]
pub struct KafkaConnection<'a, I, K> {
    id: ConnectionId,
    stream: Framed<I, KafkaCodec<'a>>,
    state: State<K>,
}

impl<'a, I, K> Deref for KafkaConnection<'a, I, K> {
    type Target = I;

    fn deref(&self) -> &Self::Target {
        self.stream.get_ref()
    }
}

impl<'a, I, K> DerefMut for KafkaConnection<'a, I, K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.stream.get_mut()
    }
}

impl<'a, I, K> Read for KafkaConnection<'a, I, K>
where
    I: AsyncRead + AsyncWrite,
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

impl<'a, I, K> Write for KafkaConnection<'a, I, K>
where
    I: AsyncRead + AsyncWrite,
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

impl<'a, I, K> AsyncRead for KafkaConnection<'a, I, K>
where
    I: AsyncRead + AsyncWrite,
{
}

impl<'a, I, K> AsyncWrite for KafkaConnection<'a, I, K>
where
    I: AsyncRead + AsyncWrite,
{
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        trace!("shutdown stream");

        self.stream.get_mut().shutdown()
    }
}

impl<'a, I, K> Stream for KafkaConnection<'a, I, K>
where
    I: AsyncRead + AsyncWrite,
{
    type Item = Frame<KafkaResponse, BytesMut, io::Error>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.stream.poll().map(|res| {
            res.map(|res| {
                res.map(|res| Frame::Message {
                    message: res,
                    body: false,
                })
            })
        })
    }
}

impl<'a, I, K> Sink for KafkaConnection<'a, I, K>
where
    I: AsyncRead + AsyncWrite,
    K: KeepAlive,
{
    type SinkItem = Frame<KafkaRequest<'a>, BytesMut, io::Error>;
    type SinkError = io::Error;

    fn start_send(&mut self, frame: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match frame {
            Frame::Message { message: request, body } => {
                trace!(
                    "send {:?} request (api version: {}) @ connection #{}",
                    request.header().api_key(),
                    request.header().api_version,
                    self.id
                );

                self.stream.start_send(request).map(|async| match async {
                    AsyncSink::Ready => AsyncSink::Ready,
                    AsyncSink::NotReady(request) => AsyncSink::NotReady(Frame::Message { message: request, body }),
                })
            }
            Frame::Body { .. } | Frame::Error { .. } => Ok(AsyncSink::Ready),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        trace!("poll complete");

        self.state.keep_alive.idle();

        self.stream.poll_complete()
    }
}

impl<'a, I, K> Transport for KafkaConnection<'a, I, K>
where
    I: AsyncRead + AsyncWrite,
    K: KeepAlive,
    Self: 'static,
{
}

impl<'a, I, K> KafkaConnection<'a, I, K>
where
    I: AsyncRead + AsyncWrite,
    K: KeepAlive,
{
    pub fn new(id: ConnectionId, stream: I, codec: KafkaCodec<'a>, keep_alive: K) -> Self {
        KafkaConnection {
            id,
            stream: stream.framed(codec),
            state: State { keep_alive },
        }
    }

    pub fn id(&self) -> ConnectionId {
        self.id
    }
}

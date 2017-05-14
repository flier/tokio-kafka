use std::io;
use std::rc::Rc;
use std::fmt::Debug;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::BytesMut;

use futures::{Async, Poll, Stream};
use futures::future::Future;
use futures::unsync::oneshot;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_core::reactor::Handle;
use tokio_proto::BindClient;
use tokio_proto::streaming::{Body, Message};
use tokio_proto::streaming::pipeline::ClientProto;
use tokio_proto::util::client_proxy::ClientProxy;
use tokio_service::Service;

use errors::Error;
use network::{ConnectionId, KafkaCodec, KafkaConnection, KafkaConnector, KafkaRequest,
              KafkaResponse, Pool, Pooled};
use client::{Metrics, StaticBoxFuture};

#[derive(Debug, Default)]
struct State {
    connection_id: ConnectionId,
}

impl State {
    pub fn next_connection_id(&mut self) -> ConnectionId {
        self.connection_id = self.connection_id.wrapping_add(1);
        self.connection_id - 1
    }
}

pub struct KafkaService<'a> {
    handle: Handle,
    pool: Pool<SocketAddr, TokioClient<'a>>,
    connector: KafkaConnector,
    metrics: Option<Rc<Metrics>>,
    state: Rc<RefCell<State>>,
}

impl<'a> KafkaService<'a> {
    pub fn new(handle: Handle,
               max_connection_idle: Duration,
               metrics: Option<Rc<Metrics>>)
               -> Self {
        KafkaService {
            handle: handle.clone(),
            pool: Pool::new(max_connection_idle),
            connector: KafkaConnector::new(handle),
            metrics: metrics,
            state: Rc::new(RefCell::new(State::default())),
        }
    }
}

impl<'a> Service for KafkaService<'a>
    where Self: 'static
{
    type Request = (SocketAddr, KafkaRequest<'a>);
    type Response = KafkaResponse;
    type Error = Error;
    type Future = FutureResponse;

    fn call(&self, req: Self::Request) -> Self::Future {
        let (addr, request) = req;
        let metrics = self.metrics.clone();

        metrics
            .as_ref()
            .map(|metrics| metrics.send_request(&request));

        let checkout = self.pool.checkout(&addr);
        let connect = {
            let handle = self.handle.clone();
            let connection_id = self.state.borrow_mut().next_connection_id();
            let pool = self.pool.clone();
            let key = Rc::new(addr);

            self.connector
                .tcp(addr)
                .map(move |io| {
                    let (tx, rx) = oneshot::channel();
                    let client = RemoteClient {
                            connection_id: connection_id,
                            client_rx: RefCell::new(Some(rx)),
                        }
                        .bind_client(&handle, io);
                    let pooled = pool.pooled(key, client);
                    drop(tx.send(pooled.clone()));
                    pooled
                })
        };

        let race = checkout
            .select(connect)
            .map(|(conn, _work)| conn)
            .map_err(|(err, _work)| {
                warn!("fail to checkout connection, {}", err);
                // the Pool Checkout cannot error, so the only error
                // is from the Connector
                // XXX: should wait on the Checkout? Problem is
                // that if the connector is failing, it may be that we
                // never had a pooled stream at all
                err.into()
            });

        let response = race.and_then(move |client| client.call(Message::WithoutBody(request)))
            .map(|msg| {
                     debug!("received message: {:?}", msg);

                     match msg {
                         Message::WithoutBody(res) |
                         Message::WithBody(res, _) => res,
                     }
                 })
            .map(|response| {
                     metrics.map(|metrics| metrics.received_response(&response));

                     response
                 })
            .map_err(Error::from);

        FutureResponse::new(response)
    }
}

pub struct FutureResponse(StaticBoxFuture<KafkaResponse>);

impl FutureResponse {
    pub fn new<F>(future: F) -> Self
        where F: Future<Item = KafkaResponse, Error = Error> + 'static
    {
        FutureResponse(StaticBoxFuture::new(future))
    }
}

impl Future for FutureResponse {
    type Item = KafkaResponse;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

type TokioBody = Body<BytesMut, io::Error>;

pub struct KafkaBody(TokioBody);

impl Stream for KafkaBody {
    type Item = BytesMut;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<BytesMut>, io::Error> {
        self.0.poll()
    }
}

type TokioClient<'a> = ClientProxy<Message<KafkaRequest<'a>, KafkaBody>,
                                   Message<KafkaResponse, TokioBody>,
                                   io::Error>;

type PooledClient<'a> = Pooled<SocketAddr, TokioClient<'a>>;

struct RemoteClient<'a> {
    connection_id: u32,
    client_rx: RefCell<Option<oneshot::Receiver<PooledClient<'a>>>>,
}

impl<'a, T> ClientProto<T> for RemoteClient<'a>
    where T: AsyncRead + AsyncWrite + Debug + 'static,
          Self: 'static
{
    type Request = KafkaRequest<'a>;
    type RequestBody = <KafkaBody as Stream>::Item;
    type Response = KafkaResponse;
    type ResponseBody = BytesMut;
    type Error = io::Error;
    type Transport = KafkaConnection<'a, T, PooledClient<'a>>;
    type BindTransport = BindingClient<'a, T>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        trace!("bind transport for {:?}", io);

        BindingClient {
            connection_id: self.connection_id,
            rx: self.client_rx
                .borrow_mut()
                .take()
                .expect("client_rx was lost"),
            io: Some(io),
        }
    }
}

struct BindingClient<'a, T> {
    connection_id: u32,
    rx: oneshot::Receiver<PooledClient<'a>>,
    io: Option<T>,
}

impl<'a, T> Future for BindingClient<'a, T>
    where T: AsyncRead + AsyncWrite + Debug + 'static
{
    type Item = KafkaConnection<'a, T, PooledClient<'a>>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll() {
            Ok(Async::Ready(client)) => {
                trace!("got connection #{} for {:?}, client {:?}",
                       self.connection_id,
                       self.io,
                       client);

                let codec = KafkaCodec::new();

                Ok(Async::Ready(KafkaConnection::new(self.connection_id,
                                                self.io.take().expect("binding client io lost"),
                                                codec,
                                                client)))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_canceled) => unreachable!(),
        }
    }
}

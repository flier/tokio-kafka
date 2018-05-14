use std::cell::RefCell;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use bytes::BytesMut;

use futures::future::Future;
use futures::unsync::oneshot;
use futures::{Async, Poll, Stream};
use tokio_core::reactor::Handle;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_proto::BindClient;
use tokio_proto::streaming::pipeline::ClientProto;
use tokio_proto::streaming::{Body, Message};
use tokio_proto::util::client_proxy::ClientProxy;
use tokio_service::Service;
use ns_router::{AutoName, Router};

use client::{Metrics, StaticBoxFuture, ToStaticBoxFuture};
use errors::Error;
use network::{ConnectionId, KafkaCodec, KafkaConnection, KafkaConnector, KafkaRequest, KafkaResponse, Pool, Pooled};

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
    pub fn new(
        handle: Handle,
        router: Rc<Router>,
        max_connection_idle: Duration,
        metrics: Option<Rc<Metrics>>,
    ) -> Self {
        KafkaService {
            handle: handle.clone(),
            pool: Pool::new(max_connection_idle),
            connector: KafkaConnector::new(handle, router),
            metrics,
            state: Rc::new(RefCell::new(State::default())),
        }
    }
}

impl<'a> Service for KafkaService<'a>
where
    Self: 'static,
{
    type Request = (SocketAddr, KafkaRequest<'a>);
    type Response = KafkaResponse;
    type Error = Error;
    type Future = FutureResponse;

    fn call(&self, req: Self::Request) -> Self::Future {
        let (addr, request) = req;

        if let Some(ref metrics) = self.metrics {
            metrics.send_request(&addr, &request);
        }

        let checkout = self.pool.checkout(addr);
        let connect = {
            let handle = self.handle.clone();
            let connection_id = self.state.borrow_mut().next_connection_id();
            let pool = self.pool.clone();

            self.connector.tcp(AutoName::SocketAddr(addr)).map(move |io| {
                let (tx, rx) = oneshot::channel();
                let client = RemoteClient {
                    connection_id,
                    client_rx: RefCell::new(Some(rx)),
                }.bind_client(&handle, io);
                let pooled = pool.pooled(addr, client);
                drop(tx.send(pooled.clone()));
                pooled
            })
        };

        let race = checkout
            .select(connect)
            .map(|(client, _work)| client)
            .map_err(|(err, _work)| {
                warn!("fail to checkout connection, {}", err);
                // the Pool Checkout cannot error, so the only error
                // is from the Connector
                // XXX: should wait on the Checkout? Problem is
                // that if the connector is failing, it may be that we
                // never had a pooled stream at all
                err
            });

        let metrics = self.metrics.clone();

        race.and_then(move |client| client.call(Message::WithoutBody(request)))
            .map(|msg| {
                debug!("received message: {:?}", msg);

                match msg {
                    Message::WithoutBody(res) | Message::WithBody(res, _) => res,
                }
            })
            .map(move |response| {
                if let Some(ref metrics) = metrics {
                    metrics.received_response(&addr, &response);
                }

                response
            })
            .from_err()
            .static_boxed()
    }
}

pub type FutureResponse = StaticBoxFuture<KafkaResponse>;

type TokioBody = Body<BytesMut, io::Error>;

pub struct KafkaBody(TokioBody);

impl Stream for KafkaBody {
    type Item = BytesMut;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<BytesMut>, io::Error> {
        self.0.poll()
    }
}

type TokioClient<'a> = ClientProxy<Message<KafkaRequest<'a>, KafkaBody>, Message<KafkaResponse, TokioBody>, io::Error>;

type PooledClient<'a> = Pooled<SocketAddr, TokioClient<'a>>;

struct RemoteClient<'a> {
    connection_id: u32,
    client_rx: RefCell<Option<oneshot::Receiver<PooledClient<'a>>>>,
}

impl<'a, T> ClientProto<T> for RemoteClient<'a>
where
    T: AsyncRead + AsyncWrite + Debug + 'static,
    Self: 'static,
{
    type Request = KafkaRequest<'a>;
    type RequestBody = <KafkaBody as Stream>::Item;
    type Response = KafkaResponse;
    type ResponseBody = BytesMut;
    type Error = io::Error;
    type Transport = KafkaConnection<'a, T, PooledClient<'a>>;
    type BindTransport = BindingClient<'a, T>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        trace!("connection #{} bind transport for {:?}", self.connection_id, io);

        BindingClient {
            connection_id: self.connection_id,
            rx: self.client_rx.borrow_mut().take().expect("client_rx was lost"),
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
where
    T: AsyncRead + AsyncWrite + Debug + 'static,
{
    type Item = KafkaConnection<'a, T, PooledClient<'a>>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll() {
            Ok(Async::Ready(client)) => {
                trace!("got connection #{} with {:?}", self.connection_id, client);

                Ok(Async::Ready(KafkaConnection::new(
                    self.connection_id,
                    self.io.take().expect("binding client io lost"),
                    KafkaCodec::new(),
                    client,
                )))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_canceled) => unreachable!(),
        }
    }
}

use std::io;
use std::rc::Rc;
use std::fmt::Debug;
use std::cell::RefCell;
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::{Deref, DerefMut};

use bytes::BytesMut;

use futures::{Async, Poll, Stream};
use futures::future::{self, Future};
use futures::unsync::oneshot;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_core::reactor::Handle;
use tokio_proto::BindClient;
use tokio_proto::streaming::{Message, Body};
use tokio_proto::streaming::pipeline::ClientProto;
use tokio_proto::util::client_proxy::ClientProxy;
use tokio_service::Service;

use network::{KafkaConnection, KafkaConnector, Pool, Pooled};
use protocol::MetadataRequest;
use network::{KafkaRequest, KafkaResponse, KafkaCodec};
use client::{KafkaConfig, KafkaState, Metadata, DEFAULT_MAX_CONNECTION_TIMEOUT};

pub struct KafkaClient {
    config: KafkaConfig,
    handle: Handle,
    connector: KafkaConnector,
    pool: Pool<SocketAddr, TokioClient>,
    state: Rc<RefCell<KafkaState>>,
}

impl Deref for KafkaClient {
    type Target = KafkaConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl DerefMut for KafkaClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl KafkaClient {
    pub fn from_config(config: KafkaConfig, handle: &Handle) -> Self {
        debug!("client with config: {:?}", config);

        let max_connection_idle = config
            .max_connection_idle()
            .unwrap_or(*DEFAULT_MAX_CONNECTION_TIMEOUT);

        KafkaClient {
            config: config,
            handle: handle.clone(),
            connector: KafkaConnector::new(handle),
            pool: Pool::new(max_connection_idle),
            state: Rc::new(RefCell::new(KafkaState::new())),
        }
    }

    pub fn from_hosts<A: ToSocketAddrs + Clone>(hosts: &[A], handle: &Handle) -> Self {
        KafkaClient::from_config(KafkaConfig::from_hosts(hosts), handle)
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    pub fn metadata(&self) -> Rc<Metadata> {
        self.state.borrow().metadata()
    }

    pub fn load_metadata(&mut self) -> LoadMetadata {
        debug!("loading metadata...");

        let state = self.state.clone();

        StaticBoxFuture::new(self.fetch_metadata::<&str>(&[])
                                 .and_then(move |metadata| {
                                               state.borrow_mut().update_metadata(metadata);

                                               future::ok(())
                                           }))
    }

    fn fetch_metadata<S>(&mut self, topic_names: &[S]) -> FetchMetadata
        where S: AsRef<str> + Debug
    {
        debug!("fetch metadata for toipcs: {:?}", topic_names);

        let addrs = self.config.brokers().unwrap();
        let addr = addrs.iter().next().unwrap();

        let checkout = self.pool.checkout(addr);
        let connect = {
            let handle = self.handle.clone();
            let pool = self.pool.clone();
            let key = Rc::new(addr.clone());

            self.connector
                .tcp(addr.clone())
                .map(move |io| {
                         let (tx, rx) = oneshot::channel();
                         let client = RemoteClient { client_rx: RefCell::new(Some(rx)) }
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

        let api_version = 0;
        let correlation_id = self.state.borrow_mut().next_correlation_id();
        let client_id = self.config.client_id();
        let request = KafkaRequest::Metadata(MetadataRequest::new(api_version,
                                                                  correlation_id,
                                                                  client_id,
                                                                  topic_names));

        let response = race.and_then(move |client| client.call(Message::WithoutBody(request)))
            .map(|msg| {
                     debug!("received message: {:?}", msg);

                     match msg {
                         Message::WithoutBody(res) => res,
                         Message::WithBody(res, _) => res,
                     }
                 })
            .and_then(|res| if let KafkaResponse::Metadata(res) = res {
                          future::ok(Metadata::from(res))
                      } else {
                          future::err(io::Error::new(io::ErrorKind::Other, "invalid response"))
                      });

        FetchMetadata::new(response)
    }
}

pub struct StaticBoxFuture<F = (), E = io::Error>(Box<Future<Item = F, Error = E> + 'static>);

impl<F, E> StaticBoxFuture<F, E> {
    pub fn new<T>(inner: T) -> Self
        where T: Future<Item = F, Error = E> + 'static
    {
        StaticBoxFuture(Box::new(inner))
    }
}

impl<F, E> Future for StaticBoxFuture<F, E> {
    type Item = F;
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

pub type LoadMetadata = StaticBoxFuture;
pub type FetchMetadata = StaticBoxFuture<Metadata>;
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

type TokioClient = ClientProxy<Message<KafkaRequest, KafkaBody>,
                               Message<KafkaResponse, TokioBody>,
                               io::Error>;

struct RemoteClient {
    client_rx: RefCell<Option<oneshot::Receiver<Pooled<SocketAddr, TokioClient>>>>,
}

impl<T> ClientProto<T> for RemoteClient
    where T: AsyncRead + AsyncWrite + Debug + 'static
{
    type Request = KafkaRequest;
    type RequestBody = <KafkaBody as Stream>::Item;
    type Response = KafkaResponse;
    type ResponseBody = BytesMut;
    type Error = io::Error;
    type Transport = KafkaConnection<T>;
    type BindTransport = BindingClient<T>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        trace!("bind transport for {:?}", io);

        BindingClient {
            rx: self.client_rx
                .borrow_mut()
                .take()
                .expect("client_rx was lost"),
            io: Some(io),
        }
    }
}

struct BindingClient<T> {
    rx: oneshot::Receiver<Pooled<SocketAddr, TokioClient>>,
    io: Option<T>,
}

impl<T> Future for BindingClient<T>
    where T: AsyncRead + AsyncWrite + Debug + 'static
{
    type Item = KafkaConnection<T>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll() {
            Ok(Async::Ready(client)) => {
                trace!("got connection for {:?}", self.io);

                let codec = KafkaCodec::new();

                Ok(Async::Ready(KafkaConnection::new(0, self.io.take().expect("binding client io lost"), codec)))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_canceled) => unreachable!(),
        }
    }
}
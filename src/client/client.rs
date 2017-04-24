use std::io;
use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::iter::FromIterator;
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::{Deref, DerefMut};

use bytes::BytesMut;

use futures::{Async, Poll, Stream};
use futures::future::{self, Future, BoxFuture, select_ok};
use futures::unsync::oneshot;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_core::reactor::Handle;
use tokio_proto::BindClient;
use tokio_proto::streaming::{Message, Body};
use tokio_proto::streaming::pipeline::ClientProto;
use tokio_proto::util::client_proxy::ClientProxy;
use tokio_service::{Service, NewService};
use tokio_timer::Timer;

use errors::{Error, ErrorKind, Result};
use network::{KafkaConnection, KafkaConnector, Pool, Pooled};
use protocol::{ApiVersion, MetadataRequest, MetadataResponse};
use network::{KafkaCodec, KafkaRequest, KafkaResponse};
use client::{KafkaOption, KafkaConfig, KafkaState, Metadata, DEFAULT_MAX_CONNECTION_TIMEOUT,
             DEFAULT_MAX_POOLED_CONNECTIONS};

pub struct KafkaClient {
    config: KafkaConfig,
    handle: Handle,
    connector: KafkaConnector,
    pool: Pool<SocketAddr, TokioClient>,
    state: KafkaState,
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
        let max_connection_idle = config
            .max_connection_idle()
            .unwrap_or(*DEFAULT_MAX_CONNECTION_TIMEOUT);

        KafkaClient {
            config: config,
            handle: handle.clone(),
            connector: KafkaConnector::new(handle),
            pool: Pool::new(max_connection_idle),
            state: KafkaState::new(),
        }
    }

    pub fn from_hosts<A: ToSocketAddrs + Clone>(hosts: &[A], handle: &Handle) -> Self {
        KafkaClient::from_config(KafkaConfig::from_hosts(hosts), handle)
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }
    /*
    pub fn load_metadata(&mut self, handle: &Handle) -> Result<()> {
        self.fetch_metadata::<&str>(&[], handle)
            .and_then(|res| if let KafkaResponse::Metadata(res) = res {
                          future::ok(Metadata::from(res))
                      } else {
                          future::err(ErrorKind::OtherError.into())
                      })
            .and_then(|metadata| {
                          //self.state.update_metadata(metadata);

                          future::ok(())
                      })
            .wait()
    }
    */
    fn fetch_metadata<S>(&mut self, topic_names: &[S], handle: &Handle) -> FutureResponse
        where S: AsRef<str>
    {
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
            .map_err(|(e, _work)| {
                         // the Pool Checkout cannot error, so the only error
                         // is from the Connector
                         // XXX: should wait on the Checkout? Problem is
                         // that if the connector is failing, it may be that we
                         // never had a pooled stream at all
                         e.into()
                     });



        let api_version = ApiVersion::Kafka_0_8;
        let correlation_id = self.state.next_correlation_id();
        let client_id = self.config.client_id();
        let request = KafkaRequest::Metadata(MetadataRequest::new(api_version,
                                                                  correlation_id,
                                                                  client_id,
                                                                  topic_names));

        let response = race.and_then(move |client| client.call(Message::WithoutBody(request)))
            .map(|msg| match msg {
                     Message::WithoutBody(res) => res,
                     Message::WithBody(res, _) => res,
                 });

        FutureResponse(Box::new(response))
    }
}

pub struct FutureResponse(Box<Future<Item = KafkaResponse, Error = io::Error> + 'static>);

impl Future for FutureResponse {
    type Item = KafkaResponse;
    type Error = io::Error;

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

type TokioClient = ClientProxy<Message<KafkaRequest, KafkaBody>,
                               Message<KafkaResponse, TokioBody>,
                               io::Error>;

struct RemoteClient {
    client_rx: RefCell<Option<oneshot::Receiver<Pooled<SocketAddr, TokioClient>>>>,
}

impl<T> ClientProto<T> for RemoteClient
    where T: AsyncRead + AsyncWrite + 'static
{
    type Request = KafkaRequest;
    type RequestBody = <KafkaBody as Stream>::Item;
    type Response = KafkaResponse;
    type ResponseBody = BytesMut;
    type Error = io::Error;
    type Transport = KafkaConnection<T>;
    type BindTransport = BindingClient<T>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
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
    where T: AsyncRead + AsyncWrite + 'static
{
    type Item = KafkaConnection<T>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll() {
            Ok(Async::Ready(client)) => {
                Ok(Async::Ready(KafkaConnection::new(0, self.io.take().expect("binding client io lost"))))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_canceled) => unreachable!(),
        }
    }
}
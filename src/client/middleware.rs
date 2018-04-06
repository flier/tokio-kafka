use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::net::SocketAddr;
use tokio_timer::{self as timer, Timer};
use std::time::Duration;
use std::rc::Rc;

use futures::Future;
use tokio_service::Service;

use client::{StaticBoxFuture, ToStaticBoxFuture};

#[derive(Clone)]
pub struct InFlightMiddleware<S> {
    upstream: S,
    state: Rc<RefCell<State>>,
}

struct State {
    requests: HashMap<SocketAddr, usize>,
}

impl State {
    pub fn send_request(&mut self, addr: SocketAddr) {
        let requests = self.requests.entry(addr).or_insert(0);

        if let Some(new) = requests.checked_add(1) {
            *requests = new;
        }
    }

    pub fn received_response(&mut self, addr: SocketAddr) {
        let requests = self.requests.entry(addr).or_insert(0);

        if let Some(new) = requests.checked_sub(1) {
            *requests = new;
        }
    }
}

impl<S> InFlightMiddleware<S> {
    pub fn new(upstream: S) -> InFlightMiddleware<S> {
        InFlightMiddleware {
            upstream: upstream,
            state: Rc::new(RefCell::new(State {
                requests: HashMap::new(),
            })),
        }
    }

    pub fn in_flight_requests(&self, addr: &SocketAddr) -> Option<usize> {
        self.state.borrow().requests.get(addr).cloned()
    }
}

impl<S> Service for InFlightMiddleware<S>
where
    Self: 'static,
    S: Service,
    S::Request: WithAddr,
    S::Error: StdError,
{
    type Request = S::Request;
    type Response = S::Response;
    type Error = S::Error;
    type Future = StaticBoxFuture<S::Response, S::Error>;

    fn call(&self, request: Self::Request) -> Self::Future {
        let addr = request.addr();
        let state = self.state.clone();

        state.borrow_mut().send_request(addr);

        self.upstream
            .call(request)
            .then(move |response| {
                state.borrow_mut().received_response(addr);

                response
            })
            .from_err()
            .static_boxed()
    }
}

pub trait WithAddr {
    fn addr(&self) -> SocketAddr;
}

impl<T> WithAddr for (SocketAddr, T) {
    fn addr(&self) -> SocketAddr {
        self.0
    }
}

/// Abort requests that are taking too long
#[derive(Clone)]
pub struct Timeout<S> {
    upstream: S,
    timer: Timer,
    duration: Duration,
}

impl<S> Timeout<S> {
    /// Crate a new `Timeout` with the given `upstream` service.
    ///
    /// Requests will be limited to `duration` and aborted once the limit has
    /// been reached.
    pub fn new(upstream: S, timer: Timer, duration: Duration) -> Timeout<S> {
        Timeout {
            upstream: upstream,
            duration: duration,
            timer: timer,
        }
    }
}

impl<S, E> Service for Timeout<S>
    where S: Service<Error = E>,
          E: From<timer::TimeoutError<S::Future>>,
{
    type Request = S::Request;
    type Response = S::Response;
    type Error = S::Error;
    type Future = timer::Timeout<S::Future>;

    fn call(&self, request: Self::Request) -> Self::Future {
        let resp = self.upstream.call(request);
        self.timer.timeout(resp, self.duration)
    }
}
use std::io;
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::Rc;
use std::cell::{Cell, RefCell};
use std::ops::{Deref, DerefMut};
use std::collections::HashMap;
use std::collections::vec_deque::VecDeque;

use std::time::{Duration, Instant};

use futures::{Async, Future, Poll};
use futures::unsync::oneshot;

use network::{KeepAlive, Status};

#[derive(Clone, Debug)]
pub struct Pool<K, T>
    where K: Clone + Hash + Eq,
          T: Clone
{
    inner: Rc<RefCell<PoolInner<K, T>>>,
}

#[derive(Debug)]
struct PoolInner<K, T>
    where K: Clone + Hash + Eq,
          T: Clone
{
    enabled: bool,
    timeout: Option<Duration>,
    idle: HashMap<Rc<K>, Vec<Entry<T>>>,
    parked: HashMap<Rc<K>, VecDeque<oneshot::Sender<Entry<T>>>>,
}

impl<K, T> Pool<K, T>
    where K: Clone + Debug + Hash + Eq,
          T: Clone
{
    pub fn new(timeout: Duration) -> Self {
        Pool {
            inner: Rc::new(RefCell::new(PoolInner {
                                            enabled: true,
                                            timeout: Some(timeout),
                                            idle: HashMap::new(),
                                            parked: HashMap::new(),
                                        })),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.borrow().enabled
    }

    pub fn enable(&mut self) {
        self.inner.borrow_mut().enabled = true
    }

    pub fn disable(&mut self) {
        self.inner.borrow_mut().enabled = false
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.inner.borrow().timeout
    }

    pub fn checkout(&self, key: &K) -> Checkout<K, T> {
        Checkout {
            key: Rc::new(key.clone()),
            pool: self.clone(),
            parked: None,
        }
    }

    fn put(&mut self, key: Rc<K>, entry: Entry<T>) {
        trace!("put {:?}", key);

        let mut inner = self.inner.borrow_mut();
        let mut remove_parked = false;
        let mut entry = Some(entry);
        if let Some(parked) = inner.parked.get_mut(&key) {
            while let Some(tx) = parked.pop_front() {
                match tx.send(entry.take().unwrap()) {
                    Ok(()) => break,
                    Err(e) => {
                        trace!("removing canceled parked {:?}", key);
                        entry = Some(e);
                    }
                }
            }
            remove_parked = parked.is_empty();
        }
        if remove_parked {
            inner.parked.remove(&key);
        }

        match entry {
            Some(entry) => {
                inner.idle.entry(key).or_insert(Vec::new()).push(entry);
            }
            None => trace!("found parked {:?}", key),
        }
    }

    pub fn pooled(&self, key: Rc<K>, value: T) -> Pooled<K, T> {
        trace!("pooled {:?}", key);

        Pooled {
            entry: Entry {
                value: value,
                reused: false,
                status: Rc::new(Cell::new(Status::Busy)),
            },
            key: key,
            pool: self.clone(),
        }
    }

    fn reuse(&self, key: Rc<K>, mut entry: Entry<T>) -> Pooled<K, T> {
        trace!("reuse {:?}", key);

        entry.reused = true;
        entry.status.set(Status::Busy);
        Pooled {
            entry: entry,
            key: key,
            pool: self.clone(),
        }
    }

    fn park(&mut self, key: Rc<K>, tx: oneshot::Sender<Entry<T>>) {
        trace!("park {:?}", key);

        self.inner
            .borrow_mut()
            .parked
            .entry(key)
            .or_insert(VecDeque::new())
            .push_back(tx);
    }
}

#[derive(Clone, Debug)]
pub struct Pooled<K, T>
    where K: Clone + Hash + Eq,
          T: Clone
{
    entry: Entry<T>,
    key: Rc<K>,
    pool: Pool<K, T>,
}

#[derive(Clone, Debug)]
struct Entry<T>
    where T: Clone
{
    value: T,
    reused: bool,
    status: Rc<Cell<Status>>,
}

impl<K, T> Deref for Pooled<K, T>
    where K: Clone + Hash + Eq,
          T: Clone
{
    type Target = T;
    fn deref(&self) -> &T {
        &self.entry.value
    }
}

impl<K, T> DerefMut for Pooled<K, T>
    where K: Clone + Hash + Eq,
          T: Clone
{
    fn deref_mut(&mut self) -> &mut T {
        &mut self.entry.value
    }
}

impl<K, T> KeepAlive for Pooled<K, T>
    where K: Clone + Debug + Hash + Eq,
          T: Clone + Debug
{
    fn status(&self) -> Status {
        self.entry.status.get()
    }

    fn busy(&mut self) {
        trace!("busy: {:?}", self);

        self.entry.status.set(Status::Busy)
    }

    fn close(&mut self) {
        trace!("closed: {:?}", self);

        self.entry.status.set(Status::Closed)
    }

    fn idle(&mut self) {
        let previous = self.status();
        self.entry.status.set(Status::Idle(Instant::now()));
        if let Status::Idle(..) = previous {
            trace!("already idle, {:?}", self);

            return;
        }
        self.entry.reused = true;
        if self.pool.is_enabled() {
            trace!("idle, {:?}", self);

            self.pool.put(self.key.clone(), self.entry.clone());
        }
    }
}

pub struct Checkout<K, T>
    where K: Clone + Hash + Eq,
          T: Clone
{
    key: Rc<K>,
    pool: Pool<K, T>,
    parked: Option<oneshot::Receiver<Entry<T>>>,
}

impl<K, T> Future for Checkout<K, T>
    where K: Clone + Debug + Hash + Eq,
          T: Clone
{
    type Item = Pooled<K, T>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut drop_parked = false;
        if let Some(ref mut rx) = self.parked {
            match rx.poll() {
                Ok(Async::Ready(entry)) => {
                    trace!("found parked for {:?}", self.key);

                    return Ok(Async::Ready(self.pool.reuse(self.key.clone(), entry)));
                }
                Ok(Async::NotReady) => (),
                Err(oneshot::Canceled) => drop_parked = true,
            }
        }
        if drop_parked {
            self.parked.take();
        }

        let expiration = Expiration::new(self.pool.inner.borrow().timeout);
        let key = &self.key;
        let mut should_remove = false;
        let entry = self.pool
            .inner
            .borrow_mut()
            .idle
            .get_mut(key)
            .and_then(|list| {
                while let Some(entry) = list.pop() {
                    match entry.status.get() {
                        Status::Idle(idle_at) if !expiration.expires(idle_at) => {
                            trace!("found idle for {:?}", key);

                            should_remove = list.is_empty();
                            return Some(entry);
                        }
                        _ => {
                            trace!("removing unacceptable pooled for {:?}", key);

                            // every other case the Entry should just be dropped
                            // 1. Idle but expired
                            // 2. Busy (something else somehow took it?)
                            // 3. Disabled don't reuse of course
                        }
                    }
                }
                should_remove = true;
                None
            });

        if should_remove {
            self.pool.inner.borrow_mut().idle.remove(key);
        }
        match entry {
            Some(entry) => Ok(Async::Ready(self.pool.reuse(self.key.clone(), entry))),
            None => {
                if self.parked.is_none() {
                    let (tx, mut rx) = oneshot::channel();
                    let _ = rx.poll(); // park this task
                    self.pool.park(self.key.clone(), tx);
                    self.parked = Some(rx);
                }
                Ok(Async::NotReady)
            }
        }
    }
}

struct Expiration(Option<Instant>);

impl Expiration {
    fn new(dur: Option<Duration>) -> Expiration {
        Expiration(dur.map(|dur| Instant::now() - dur))
    }

    fn expires(&self, instant: Instant) -> bool {
        match self.0 {
            Some(expire) => expire > instant,
            None => false,
        }
    }
}
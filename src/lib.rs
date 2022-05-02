use futures_channel::oneshot::Receiver;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::task::Poll;

mod inner;

use inner::CallHandle;
use inner::MultiRwLockInner;

pub struct MultiRwLock<T: Send> {
    resources: RwLock<HashMap<usize, Box<T>>>,
    inner: Mutex<MultiRwLockInner>,
    next_handle: AtomicUsize,
    next_resource_id: AtomicUsize,
}

impl<T: Send> MultiRwLock<T> {
    pub fn new(max_readers: usize, max_draining: usize) -> Self {
        let resources = HashMap::new();
        let resources = RwLock::new(resources);
        let inner = MultiRwLockInner::new(max_readers, max_draining);
        let inner = Mutex::new(inner);
        let next_handle = AtomicUsize::new(0);
        let next_resource_id = AtomicUsize::new(0);
        Self {
            resources,
            inner,
            next_handle,
            next_resource_id,
        }
    }

    pub fn push(&self, resource: T) {
        let res_idx = self
            .next_resource_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut lock = self.resources.write();
        lock.insert(res_idx, Box::new(resource));
        drop(lock);
        let mut lock = self.inner.lock();
        lock.push_resource(res_idx);
        drop(lock);
    }

    pub fn read(&self) -> ReadFuture<'_, T> {
        let handle = self
            .next_handle
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut lock = self.inner.lock();
        let resource_id = lock.read(handle);
        drop(lock);

        ReadFuture {
            handle,
            resources: &self.resources,
            inner: &self.inner,
            resource_id,
        }
    }

    pub fn write(&self) -> WriteFuture<'_, T> {
        let handle = self
            .next_handle
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut lock = self.inner.lock();
        let resource_id = lock.write(handle);
        drop(lock);

        WriteFuture {
            handle,
            resources: &self.resources,
            inner: &self.inner,
            resource_id,
        }
    }

    pub fn take(&self) -> TakeFuture<'_, T> {
        let handle = self
            .next_handle
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut lock = self.inner.lock();
        let resource_id = lock.write(handle);
        drop(lock);

        TakeFuture {
            handle,
            resources: &self.resources,
            inner: &self.inner,
            resource_id,
        }
    }

    pub fn set_max_readers(&self, max: usize) {
        self.inner.lock().set_max_readers(max);
    }
}

pub struct ReadFuture<'a, T: Send> {
    handle: CallHandle,
    resources: &'a RwLock<HashMap<usize, Box<T>>>,
    inner: &'a Mutex<MultiRwLockInner>,
    resource_id: Result<usize, Receiver<usize>>,
}

impl<'a, T: Send> Future for ReadFuture<'a, T> {
    type Output = ReadGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let res_idx = match &mut this.resource_id {
            Ok(res_idx) => *res_idx,
            Err(recv) => match Pin::new(recv).poll(cx) {
                Poll::Ready(Ok(res_idx)) => res_idx,
                Poll::Ready(Err(_)) => panic!(),
                Poll::Pending => todo!(),
            },
        };

        // this is locked as a read because we are never
        // looking up the same index on two threads.
        let lock = this.resources.read();
        let t = lock.get(&res_idx).unwrap();
        let t: std::ptr::NonNull<T> = t.as_ref().into();
        let t = unsafe { t.as_ref() };
        drop(lock);

        Poll::Ready(ReadGuard {
            t,
            handle: this.handle,
            inner: this.inner,
        })
    }
}

impl<'a, T: Send> Drop for ReadFuture<'a, T> {
    fn drop(&mut self) {
        let mut lock = self.inner.lock();
        lock.unlock_read(self.handle);
        drop(lock);
    }
}

pub struct ReadGuard<'a, T: Send> {
    t: &'a T,
    handle: CallHandle,
    inner: &'a Mutex<MultiRwLockInner>,
}

impl<'a, T: Send> Deref for ReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.t
    }
}

impl<'a, T: Send> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) {
        let mut lock = self.inner.lock();
        lock.unlock_read(self.handle);
        drop(lock);
    }
}

pub struct WriteFuture<'a, T: Send> {
    handle: CallHandle,
    resources: &'a RwLock<HashMap<usize, Box<T>>>,
    inner: &'a Mutex<MultiRwLockInner>,
    resource_id: Result<usize, Receiver<usize>>,
}

impl<'a, T: Send> Future for WriteFuture<'a, T> {
    type Output = WriteGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let res_idx = match &mut this.resource_id {
            Ok(res_idx) => *res_idx,
            Err(recv) => match Pin::new(recv).poll(cx) {
                Poll::Ready(Ok(res_idx)) => res_idx,
                Poll::Ready(Err(_)) => panic!(),
                Poll::Pending => todo!(),
            },
        };

        // this is locked as a read because we are never
        // looking up the same index on two threads.
        let lock = this.resources.read();
        let t = lock.get(&res_idx).unwrap();
        let mut t: std::ptr::NonNull<T> = t.as_ref().into();
        let t = unsafe { t.as_mut() };
        drop(lock);

        Poll::Ready(WriteGuard {
            t,
            handle: this.handle,
            inner: this.inner,
        })
    }
}

impl<'a, T: Send> Drop for WriteFuture<'a, T> {
    fn drop(&mut self) {
        let mut lock = self.inner.lock();
        lock.unlock_write(self.handle);
        drop(lock);
    }
}

pub struct WriteGuard<'a, T: Send> {
    t: &'a mut T,
    handle: CallHandle,
    inner: &'a Mutex<MultiRwLockInner>,
}

impl<'a, T: Send> Deref for WriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.t
    }
}

impl<'a, T: Send> DerefMut for WriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.t
    }
}

impl<'a, T: Send> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        let mut lock = self.inner.lock();
        lock.unlock_write(self.handle);
        drop(lock);
    }
}

pub struct TakeFuture<'a, T: Send> {
    handle: CallHandle,
    resources: &'a RwLock<HashMap<usize, Box<T>>>,
    inner: &'a Mutex<MultiRwLockInner>,
    resource_id: Result<usize, Receiver<usize>>,
}

impl<'a, T: Send> Future for TakeFuture<'a, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let res_idx = match &mut this.resource_id {
            Ok(res_idx) => *res_idx,
            Err(recv) => match Pin::new(recv).poll(cx) {
                Poll::Ready(Ok(res_idx)) => res_idx,
                Poll::Ready(Err(_)) => panic!(),
                Poll::Pending => todo!(),
            },
        };

        // this is locked as a read because we are never
        // looking up the same index on two threads.
        let mut lock = this.resources.write();
        let t = lock.remove(&res_idx).unwrap();
        drop(lock);
        let t = *t;

        Poll::Ready(t)
    }
}

impl<'a, T: Send> Drop for TakeFuture<'a, T> {
    fn drop(&mut self) {
        let mut lock = self.inner.lock();
        lock.unlock_write(self.handle);
        drop(lock);
    }
}

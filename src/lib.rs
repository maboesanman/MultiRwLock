use parking_lot::Mutex;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicUsize;

mod inner;

use inner::CallHandle;
use inner::MultiRwLockInner;

pub struct MultiRwLock<T: Send> {
    resources: Vec<Box<T>>,
    inner: Mutex<MultiRwLockInner>,
    next_handle: AtomicUsize,
}

impl<T: Send> MultiRwLock<T> {
    pub fn new(resources: Vec<Box<T>>, max_readers: usize, max_draining: usize) -> Self {
        let free = resources.iter().enumerate().map(|(i, _)| i).collect();
        let inner = MultiRwLockInner::new(free, max_readers, max_draining);
        let inner = Mutex::new(inner);
        let next_handle = AtomicUsize::new(0);
        Self {
            resources,
            inner,
            next_handle,
        }
    }

    pub async fn read(&self) -> ReadGuard<'_, T> {
        let handle = self
            .next_handle
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut lock = self.inner.lock();
        let result = lock.read(handle);
        drop(lock);
        let res_idx = match result {
            Ok(res_idx) => res_idx,
            Err(recv) => recv.await.unwrap(),
        };

        let t = self.resources.get(res_idx).unwrap();

        ReadGuard {
            t,
            handle,
            inner: &self.inner,
        }
    }

    pub async fn write(&self) -> WriteGuard<'_, T> {
        let handle = self
            .next_handle
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut lock = self.inner.lock();
        let result = lock.write(handle);
        drop(lock);
        let res_idx = match result {
            Ok(res_idx) => res_idx,
            Err(recv) => recv.await.unwrap(),
        };

        let t = self.resources.get(res_idx).unwrap();
        let mut t: std::ptr::NonNull<T> = t.as_ref().into();
        // SAFETY: our MultiRwLockInner only gives out indices to writer if all other indices are full.
        let t = unsafe { t.as_mut() };
        WriteGuard {
            t,
            handle,
            inner: &self.inner,
        }
    }

    pub fn set_max_readers(&self, max: usize) {
        self.inner.lock().set_max_readers(max);
    }
}

pub struct ReadGuard<'a, T: Send> {
    t: &'a T,
    handle: CallHandle,
    inner: &'a Mutex<MultiRwLockInner>,
}

pub struct WriteGuard<'a, T: Send> {
    t: &'a mut T,
    handle: CallHandle,
    inner: &'a Mutex<MultiRwLockInner>,
}

impl<'a, T: Send> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) {
        let mut lock = self.inner.lock();
        lock.unlock_read(self.handle);
        drop(lock);
    }
}

impl<'a, T: Send> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        let mut lock = self.inner.lock();
        lock.unlock_write(self.handle);
        drop(lock);
    }
}

impl<'a, T: Send> Deref for ReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.t
    }
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

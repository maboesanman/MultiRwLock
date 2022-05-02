use std::{
    collections::{HashMap, VecDeque},
    ptr::NonNull,
};

use futures_channel::oneshot::{channel, Receiver, Sender};

use super::{CallHandle, ResourceIndex};

type UnpoppedIndex = usize;

pub struct CallQueue {
    draining_writers: VecDeque<Option<Draining>>,
    draining_writers_map: HashMap<CallHandle, UnpoppedIndex>,
    draining_writers_popped: usize,

    queue: VecDeque<Option<Queued>>,
    queue_map: HashMap<CallHandle, UnpoppedIndex>,
    queue_popped: usize,
}

pub struct Draining(CallHandle, Sender<ResourceIndex>);

pub enum Queued {
    Read(CallHandle, Sender<ResourceIndex>),
    Write(CallHandle, Sender<ResourceIndex>),
}

impl CallQueue {
    pub fn new() -> Self {
        Self {
            draining_writers: VecDeque::new(),
            draining_writers_map: HashMap::new(),
            draining_writers_popped: 0,
            queue: VecDeque::new(),
            queue_map: HashMap::new(),
            queue_popped: 0,
        }
    }

    pub fn top_is_read(&mut self) -> bool {
        matches!(self.top(), Some(Queued::Read(_, _)))
    }

    pub fn top_is_write(&mut self) -> bool {
        matches!(self.top(), Some(Queued::Write(_, _)))
    }

    fn top(&mut self) -> Option<&Queued> {
        loop {
            if let Some(f) = self.queue.front().map(Option::as_ref).flatten() {
                let f: NonNull<Queued> = f.into();
                // grumble grumble polonius
                let f = unsafe { f.as_ref() };
                break Some(f);
            } else if self.pop_front_queue().is_none() {
                break None;
            }
        }
    }

    /// pop the front only if it is a read.
    pub fn pop_front_read(&mut self) -> Option<(CallHandle, Sender<ResourceIndex>)> {
        if !self.top_is_read() {
            return None;
        }

        if let Some(Queued::Read(h, send)) = self.pop_front_queue() {
            Some((h, send))
        } else {
            unreachable!()
        }
    }

    /// pop the front. bool is true if need to unmark a draining thing.
    pub fn pop_front(&mut self) -> Option<Queued> {
        if let Some(Draining(h, s)) = self.pop_front_draining_writers() {
            return Some(Queued::Write(h, s));
        }

        self.pop_front_queue()
    }

    pub fn enqueue_read(&mut self, handle: CallHandle) -> Receiver<ResourceIndex> {
        let (send, recv) = channel();
        self.push_back_queue(true, handle, send);
        recv
    }

    pub fn enqueue_write(&mut self, handle: CallHandle) -> Receiver<ResourceIndex> {
        let (send, recv) = channel();
        self.push_back_queue(false, handle, send);
        recv
    }

    /// try to drain, returning true for drain queued, and false for reader at the top of queue or queue empty
    pub fn drain(&mut self) {
        let front = loop {
            if let Some(f) = self.queue.front().map(Option::as_ref).flatten() {
                break f;
            }
            if self.pop_front_queue().is_none() {
                panic!()
            }
        };
        match front {
            Queued::Write(_, _) => {
                if let Some(Queued::Write(handle, send)) = self.pop_front_queue() {
                    self.push_back_draining_writers(handle, send);
                }
            }
            _ => panic!(),
        }
    }

    /// returns true if a drain should be stopped.
    pub fn cancel_call(&mut self, handle: CallHandle) -> bool {
        if let Some(i) = self.queue_map.remove(&handle) {
            *self.queue.get_mut(i - self.queue_popped).unwrap() = None;
            false
        } else if let Some(i) = self.draining_writers_map.remove(&handle) {
            *self
                .draining_writers
                .get_mut(i - self.draining_writers_popped)
                .unwrap() = None;
            true
        } else {
            false
        }
    }

    fn push_back_queue(&mut self, read: bool, handle: CallHandle, send: Sender<ResourceIndex>) {
        let unpopped_index = self.queue_popped + self.queue.len();
        let queued = if read {
            Queued::Read(handle, send)
        } else {
            Queued::Write(handle, send)
        };
        self.queue.push_back(Some(queued));
        self.queue_map.insert(handle, unpopped_index);
    }

    fn pop_front_queue(&mut self) -> Option<Queued> {
        let front = self.queue.pop_front()?;
        self.queue_popped += 1;
        let front = front?;
        let handle = match front {
            Queued::Read(h, _) => h,
            Queued::Write(h, _) => h,
        };
        self.queue_map.remove(&handle);

        Some(front)
    }

    fn push_back_draining_writers(&mut self, handle: CallHandle, send: Sender<ResourceIndex>) {
        let unpopped_index = self.draining_writers_popped + self.draining_writers.len();
        let queued = Draining(handle, send);
        self.draining_writers.push_back(Some(queued));
        self.draining_writers_map.insert(handle, unpopped_index);
    }

    fn pop_front_draining_writers(&mut self) -> Option<Draining> {
        let front = self.draining_writers.pop_front()?;
        self.draining_writers_popped += 1;
        let front = front?;
        let handle = front.0;
        self.draining_writers_map.remove(&handle);

        Some(front)
    }
}

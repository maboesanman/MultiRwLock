use futures_channel::oneshot::Receiver;
use std::collections::VecDeque;

use self::queue::CallQueue;
use self::queue::Queued;
use self::reader_handles::ReaderHandles;
use self::writer_handles::WriterHandles;

mod queue;
mod reader_handles;
mod writer_handles;

// the index for a call (first call to read/write is 0, second is 1, etc...)
pub type CallHandle = usize;
pub type ResourceIndex = usize;

pub struct MultiRwLockInner {
    free: VecDeque<ResourceIndex>,
    reader_handles: ReaderHandles,
    writer_handles: WriterHandles,
    queue: CallQueue,
}

impl MultiRwLockInner {
    pub fn new(free: VecDeque<ResourceIndex>, max_readers: usize, max_draining: usize) -> Self {
        Self {
            free,
            reader_handles: ReaderHandles::new(max_readers, max_draining),
            writer_handles: WriterHandles::new(),
            queue: CallQueue::new(),
        }
    }

    // pub fn add_resource(&mut self, i: ResourceIndex) {
    //     self.handle_newly_freed_resource(i)
    // }

    pub fn read(&mut self, handle: CallHandle) -> Result<ResourceIndex, Receiver<ResourceIndex>> {
        let free = self.free.pop_front();
        if let Some(res_idx) = free {
            self.reader_handles
                .add_reader_with_resource(handle, res_idx);
            return Ok(res_idx);
        }

        if let Ok(res_idx) = self.reader_handles.add_reader(handle) {
            return Ok(res_idx);
        }

        let recv = self.queue.enqueue_read(handle);

        Err(recv)
    }

    pub fn write(&mut self, handle: CallHandle) -> Result<ResourceIndex, Receiver<ResourceIndex>> {
        let free = self.free.pop_front();
        if let Some(res_idx) = free {
            self.writer_handles
                .add_writer_with_resource(handle, res_idx);
            return Ok(res_idx);
        }

        let recv = self.queue.enqueue_write(handle);
        if self.queue.top_is_write() {
            if let Ok(()) = self.reader_handles.start_draining_reader() {
                self.queue.drain();
                return Err(recv);
            }
        }

        Err(recv)
    }

    pub fn unlock_read(&mut self, handle: CallHandle) {
        let res_idx = None;
        let res_idx = res_idx.or_else(|| self.reader_handles.remove_reader(handle));
        let res_idx = res_idx.or_else(|| {
            self.queue.cancel_call(handle);
            None
        });
        let res_idx = res_idx.or_else(|| self.free.pop_front());
        let res_idx = match res_idx {
            Some(res_idx) => res_idx,
            None => {
                if !self.reader_handles.full() {
                    loop {
                        let (h, s) = if let Some(x) = self.queue.pop_front_read() {
                            x
                        } else {
                            break;
                        };

                        let res_idx = self.reader_handles.add_reader(h).unwrap();
                        if s.send(res_idx).is_ok() {
                            break;
                        }
                        self.reader_handles.remove_reader(h);
                    }
                }
                return;
            }
        };

        self.handle_newly_freed_resource(res_idx);
    }

    pub fn unlock_write(&mut self, handle: CallHandle) {
        let res_idx = None;
        let res_idx = res_idx.or_else(|| self.writer_handles.remove_writer(handle));
        let res_idx = res_idx.or_else(|| {
            let drain_freed = self.queue.cancel_call(handle);
            if drain_freed {
                self.reader_handles.stop_draining_reader().unwrap();
            }

            None
        });
        let res_idx = res_idx.or_else(|| self.free.pop_front());
        let res_idx = match res_idx {
            Some(res_idx) => res_idx,
            None => return,
        };

        self.handle_newly_freed_resource(res_idx);
    }

    pub fn set_max_readers(&mut self, max: usize) {
        self.reader_handles.set_max_readers(max);
    }

    fn handle_newly_freed_resource(&mut self, res_idx: ResourceIndex) {
        loop {
            let queued = match self.queue.pop_front() {
                Some(x) => x,
                None => {
                    self.free.push_back(res_idx);
                    break;
                }
            };

            match queued {
                Queued::Read(h, s) => {
                    if s.send(res_idx).is_ok() {
                        self.reader_handles.add_reader_with_resource(h, res_idx);
                        break;
                    }
                }
                Queued::Write(h, s) => {
                    if s.send(res_idx).is_ok() {
                        self.writer_handles.add_writer_with_resource(h, res_idx);
                        break;
                    }
                }
            }
        }
    }
}

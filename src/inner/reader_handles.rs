use std::collections::HashMap;

use super::CallHandle;
use super::ResourceIndex;

type HeapIndex = usize;

/// this struct maintains the core structure for multiple readers.
/// when a write needs to occur but there are no T available, the min of the
/// reading heap is moved to the draining heap, and reads will no longer be
/// given out on it.
///
/// when min of reading dips below min of draining, both are popped and pushed to eachother.
pub struct ReaderHandles {
    max_draining: usize,
    // min heap on the number of readers for each reading resource
    reading: ResourceHeap,

    // min heap on the number of readers for each draining resource
    draining: ResourceHeap,

    // which readers are matched to which resources
    handles: HashMap<CallHandle, ResourceIndex>,
}

impl ReaderHandles {
    pub fn new(max_readers: usize, max_draining: usize) -> Self {
        ReaderHandles {
            max_draining,
            reading: ResourceHeap::new(max_readers),
            draining: ResourceHeap::new(max_readers),
            handles: HashMap::new(),
        }
    }

    /// don't need to return the resource index, because it's just the one passed in.
    pub fn add_reader_with_resource(&mut self, handle: CallHandle, resource: ResourceIndex) {
        self.reading.push_reader_with_resource(resource);
        self.handles.insert(handle, resource);
    }

    pub fn add_reader(&mut self, handle: CallHandle) -> Result<ResourceIndex, ()> {
        let resource = self.reading.try_push_reader()?;
        self.handles.insert(handle, resource);
        Ok(resource)
    }

    pub fn full(&self) -> bool {
        self.reading.full()
    }

    /// remove a reader (because the lock was released).
    /// returns a resource index if that resource has no more readers.
    pub fn remove_reader(&mut self, handle: CallHandle) -> Option<ResourceIndex> {
        let res_idx = *self.handles.get(&handle)?;

        self.reading
            .decrement_count(res_idx)
            .or_else(|()| self.draining.decrement_count(res_idx))
            .unwrap()
    }

    /// move a reading resource to be ready to drain
    pub fn start_draining_reader(&mut self) -> Result<(), ()> {
        if self.draining.len() == self.max_draining {
            return Err(());
        }
        let entry = self.reading.pop_entry()?;
        self.draining.push_entry(entry);
        Ok(())
    }

    /// the future for the writer was likely dropped, so don't worry about draining now.
    pub fn stop_draining_reader(&mut self) -> Result<(), ()> {
        let entry = self.draining.pop_from_back()?;
        self.reading.push_entry(entry);
        Ok(())
    }

    pub fn set_max_readers(&mut self, max_count: usize) {
        self.reading.set_max_readers(max_count);
        self.draining.set_max_readers(max_count);
    }
}

struct ResourceHeap {
    max_count: usize,
    heap: Vec<(usize, ResourceIndex)>,
    handles: HashMap<ResourceIndex, HeapIndex>,
}

impl ResourceHeap {
    pub fn new(max_count: usize) -> Self {
        ResourceHeap {
            max_count,
            heap: Vec::new(),
            handles: HashMap::new(),
        }
    }

    pub fn full(&self) -> bool {
        if let Some((count, _)) = self.heap.first() {
            *count >= self.max_count
        } else {
            true
        }
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn push_reader_with_resource(&mut self, resource: ResourceIndex) {
        self.push_entry((1, resource));
    }

    pub fn push_entry(&mut self, entry: (usize, ResourceIndex)) {
        self.heap.push(entry);
        let i = self.heap.len() - 1;
        self.handles.insert(entry.0, i);
        self.heapify_down(i);
    }

    pub fn pop_entry(&mut self) -> Result<(usize, ResourceIndex), ()> {
        self.remove(0)
    }

    pub fn pop_from_back(&mut self) -> Result<(usize, ResourceIndex), ()> {
        self.remove(self.heap.len() - 1)
    }

    pub fn try_push_reader(&mut self) -> Result<ResourceIndex, ()> {
        let first = self.heap.first_mut().ok_or(())?;
        if first.0 >= self.max_count {
            return Err(());
        }

        let index = first.1;
        first.0 += 1;
        self.heapify_up(0);
        Ok(index)
    }

    pub fn decrement_count(&mut self, res_idx: ResourceIndex) -> Result<Option<ResourceIndex>, ()> {
        let heap_idx = *self.handles.get(&res_idx).ok_or(())?;
        let heap_item = self.heap.get_mut(heap_idx).unwrap();
        heap_item.0 -= 1;
        if heap_item.0 == 0 {
            Ok(Some(self.remove(heap_idx).unwrap().1))
        } else {
            self.heapify_down(heap_idx);
            Ok(None)
        }
    }

    pub fn set_max_readers(&mut self, max_count: usize) {
        self.max_count = max_count;
    }

    fn remove(&mut self, i: HeapIndex) -> Result<(usize, ResourceIndex), ()> {
        if self.heap.len() != i {
            self.swap(i, self.heap.len());
        }

        let entry = self.heap.pop().ok_or(())?;
        self.handles.remove(&entry.1);
        self.heapify_up(i);

        Ok(entry)
    }

    fn swap(&mut self, a: HeapIndex, b: HeapIndex) {
        self.heap.swap(a, b);
        let res_idx_a = self.heap.get(a).unwrap().1;
        let res_idx_b = self.heap.get(b).unwrap().1;
        self.handles.insert(res_idx_a, a).unwrap();
        self.handles.insert(res_idx_b, b).unwrap();
    }

    // index may be less than parent, correct this.
    fn heapify_down(&mut self, index: HeapIndex) {
        if index >= self.heap.len() {
            return;
        }

        let mut c = index;
        loop {
            match Self::parent(c) {
                Some(p) => {
                    let c_count = self.heap.get(c).unwrap().0;
                    let p_count = self.heap.get(p).unwrap().0;
                    if c_count < p_count {
                        self.swap(c, p);
                        c = p;
                        continue;
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }
    }

    // index may be more than children. correct this.
    fn heapify_up(&mut self, index: HeapIndex) {
        if index >= self.heap.len() {
            return;
        }
        let mut p = index;

        loop {
            let [c1, c2] = Self::children(p);

            let p_count = self.heap.get(p).unwrap().0;
            let c1_count = self.heap.get(c1).map(|(count, _)| count);
            let c2_count = self.heap.get(c2).map(|(count, _)| count);

            match (c1_count, c2_count) {
                (None, None) => break,
                (Some(c1_count), None) => {
                    if p_count > *c1_count {
                        self.swap(p, c1);
                    }
                    break;
                }
                (Some(c1_count), Some(c2_count)) => {
                    if p_count <= *c1_count && p <= *c2_count {
                        break;
                    }

                    if c1_count < c2_count {
                        self.swap(p, c1);
                        p = c1;
                    } else {
                        self.swap(p, c2);
                        p = c2;
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    fn parent(index: HeapIndex) -> Option<HeapIndex> {
        if index == 0 {
            None
        } else {
            Some((index - 1) >> 1)
        }
    }

    fn children(index: HeapIndex) -> [HeapIndex; 2] {
        let c = (index << 1) + 1;
        [c, c + 1]
    }
}

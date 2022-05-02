use std::collections::HashMap;

use super::{CallHandle, ResourceIndex};

pub struct WriterHandles {
    writer_handles: HashMap<CallHandle, ResourceIndex>,
}

impl WriterHandles {
    pub fn new() -> Self {
        Self {
            writer_handles: HashMap::new(),
        }
    }

    pub fn add_writer_with_resource(&mut self, handle: CallHandle, resource: ResourceIndex) {
        self.writer_handles.insert(handle, resource);
    }

    pub fn remove_writer(&mut self, handle: CallHandle) -> Option<ResourceIndex> {
        self.writer_handles.remove(&handle)
    }
}

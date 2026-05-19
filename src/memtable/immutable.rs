use crate::memtable::table::Memtable;
use std::sync::Arc;

pub struct ImmutableMemtable {
    inner: Arc<Memtable>,
}

impl ImmutableMemtable {
    pub fn new(memtable: Memtable) -> Self {
        Self {
            inner: Arc::new(memtable),
        }
    }

    pub fn inner(&self) -> Arc<Memtable> {
        Arc::clone(&self.inner)
    }
}

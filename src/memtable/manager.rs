use crate::memtable::immutable::ImmutableMemtable;
use crate::memtable::table::Memtable;
use std::sync::{Arc, Mutex};

pub struct MemtableManager {
    active: Mutex<Memtable>,
    immutable: Mutex<Vec<ImmutableMemtable>>,
    rotate_threshold: usize,
}

impl MemtableManager {
    pub fn new(rotate_threshold: usize) -> Self {
        Self {
            active: Mutex::new(Memtable::new()),
            immutable: Mutex::new(Vec::new()),
            rotate_threshold,
        }
    }

    pub fn insert(&self, series_id: u64, timestamp: i64, value: f64) {
        let mut active = self.active.lock().unwrap();

        active.insert(series_id, timestamp, value);

        if active.total_points() >= self.rotate_threshold {
            let rotated = std::mem::replace(&mut *active, Memtable::new());

            self.immutable
                .lock()
                .unwrap()
                .push(ImmutableMemtable::new(rotated));
        }
    }

    pub fn immutable_tables(&self) -> Vec<ImmutableMemtable> {
        self.immutable.lock().unwrap().drain(..).collect()
    }
}

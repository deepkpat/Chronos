use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct CardinalityTracker {
    // maps tag_key -> thread safe atomic counter
    counts: DashMap<String, AtomicUsize>,
}

impl CardinalityTracker {
    pub fn new() -> Self {
        Self {
            counts: DashMap::new(),
        }
    }

    pub fn record_tag_value(&self, key: &str) {
        if let Some(counter) = self.counts.get(key) {
            counter.value().fetch_add(1, Ordering::Relaxed);
            return;
        }

        self.counts
            .entry(key.to_string())
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_count(&self, key: &str) -> usize {
        self.counts
            .get(key)
            .map(|counter| counter.value().load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

impl Default for CardinalityTracker {
    fn default() -> Self {
        Self::new()
    }
}

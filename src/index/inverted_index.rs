use std::collections::HashSet;

use dashmap::DashMap;

pub struct InvertedIndex {
    // shared map mapping (tag_key, tag_value) -> set of series_ids
    postings: DashMap<String, HashSet<u64>>,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            postings: DashMap::new(),
        }
    }

    pub fn insert(&self, series_id: u64, tags: &[(String, String)]) {
        for (k, v) in tags {
            let storage_key = format!("{}={}", k, v);

            self.postings
                .entry(storage_key)
                .or_default()
                .insert(series_id);
        }
    }

    pub fn lookup(&self, key: &str, value: &str) -> HashSet<u64> {
        let query = format!("{}={}", key, value);

        self.postings
            .get(&query)
            .map(|ref_multi| ref_multi.value().clone())
            .unwrap_or_default()
    }
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

use crate::types::Series;
use dashmap::DashMap;

pub struct MetadataStore {
    // maps series_id -> series metadata
    series: DashMap<u64, Series>,
}

impl MetadataStore {
    pub fn new() -> Self {
        Self {
            series: DashMap::new(),
        }
    }

    pub fn insert(&self, series: Series) {
        self.series.insert(series.id, series);
    }

    pub fn get(&self, id: u64) -> Option<Series> {
        self.series
            .get(&id)
            .map(|ref_multi| ref_multi.value().clone())
    }
}

impl Default for MetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

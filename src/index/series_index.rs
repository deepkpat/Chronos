use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::types::{Series, Tags};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SeriesKey {
    pub metric: String,
    pub tags: Vec<(String, String)>,
}

impl From<(&str, &Tags)> for SeriesKey {
    fn from((metric, tags): (&str, &Tags)) -> Self {
        Self {
            metric: metric.to_string(),
            tags: tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        }
    }
}

pub struct SeriesIndex {
    next_id: AtomicU64,
    key_to_id: DashMap<SeriesKey, u64>,
    id_to_series: DashMap<u64, Series>,
}

impl SeriesIndex {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            key_to_id: DashMap::new(),
            id_to_series: DashMap::new(),
        }
    }

    pub fn get_or_create(&self, metric: &str, tags: &Tags) -> u64 {
        let key = SeriesKey::from((metric, tags));

        let mut is_new = false;

        let entry = self.key_to_id.entry(key).or_insert_with(|| {
            is_new = true;
            self.next_id.fetch_add(1, Ordering::Relaxed)
        });

        let id = *entry.value();

        drop(entry);

        if is_new {
            self.id_to_series.insert(
                id,
                Series {
                    id,
                    metric: metric.to_string(),
                    tags: tags.clone(),
                },
            );
        }

        id
    }

    pub fn get_series(&self, id: u64) -> Option<Series> {
        self.id_to_series
            .get(&id)
            .map(|ref_multi| ref_multi.value().clone())
    }
}

impl Default for SeriesIndex {
    fn default() -> Self {
        Self::new()
    }
}

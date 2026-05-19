use std::collections::BTreeMap;

use crate::types::SampleReading;

pub struct Memtable {
    series: BTreeMap<u64, Vec<SampleReading>>,
    total_points: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            series: BTreeMap::new(),
            total_points: 0,
        }
    }

    pub fn insert(&mut self, series_id: u64, timestamp: i64, value: f64) {
        self.series
            .entry(series_id)
            .or_insert_with(Vec::new)
            .push(SampleReading { timestamp, value });

        self.total_points += 1;
    }

    pub fn series(&self) -> &BTreeMap<u64, Vec<SampleReading>> {
        &self.series
    }

    pub fn total_points(&self) -> usize {
        self.total_points
    }
}

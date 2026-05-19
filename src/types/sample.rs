use crate::types::Tags;

#[derive(Debug, Clone)]
pub struct Sample {
    pub key: SeriesKey,
    pub reading: SampleReading,
}

#[derive(Debug, Clone)]
pub struct SeriesKey {
    pub metric: String,
    pub tags: Tags,
}

#[derive(Debug, Clone)]
pub struct SampleReading {
    pub timestamp: i64,
    pub value: f64,
}

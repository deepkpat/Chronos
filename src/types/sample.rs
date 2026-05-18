use crate::types::Tags;

#[derive(Debug, Clone)]
pub struct Sample {
    pub metric: String,
    pub tags: Tags,
    pub timestamp: i64,
    pub value: f64,
}

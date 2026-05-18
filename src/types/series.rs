use crate::types::Tags;

#[derive(Debug, Clone)]
pub struct Series {
    pub id: u64,
    pub metric: String,
    pub tags: Tags,
}

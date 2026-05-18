#[derive(Debug, Clone)]
pub struct TagFilter {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub metric: String,
    pub filters: Vec<TagFilter>,
    pub start: i64,
    pub end: i64,
}

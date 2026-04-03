use serde::{Deserialize, Serialize};

use crate::types::{ChunkId, SeriesId};

/// chunk metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub chunk_id: ChunkId,
    pub series_id: SeriesId,
    pub start_time: u64,
    pub end_time: u64,
}

/// chunk data points
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkData {
    pub timestamps: Vec<u64>,
    pub values: Vec<f64>,
}

/// chunk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub meta: ChunkMeta,
    pub data: ChunkData,
}

impl Chunk {}

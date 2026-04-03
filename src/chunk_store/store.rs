use async_trait::async_trait;

use crate::chunk::Chunk;
use crate::errors::ChronosResult;
use crate::types::ChunkId;

/// trait for storing and retrieving chunk
#[async_trait]
pub trait ChunkStore: Send + Sync {
    /// save a chunk
    async fn save(&self, chunk: &Chunk) -> ChronosResult<()>;

    /// load a chunk by id
    async fn load(&self, chunk_id: ChunkId) -> ChronosResult<Option<Chunk>>;

    /// delete a chunk by id
    async fn delete(&self, chunk_id: ChunkId) -> ChronosResult<()>;

    /// Check if a chunk exists
    async fn exists(&self, chunk_id: ChunkId) -> ChronosResult<bool>;
}

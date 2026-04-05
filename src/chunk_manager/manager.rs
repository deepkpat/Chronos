use std::{ops::Deref, sync::Arc};

use async_trait::async_trait;

use crate::{chunk::Chunk, errors::ChronosResult, types::ChunkId};

/// trait to release chunks back to manager
pub trait ChunkReleaser: Send + Sync {
    fn release(&self, chunk_id: ChunkId);
}

/// RAII guard for chunk access, release the chunk when dropped
pub struct ChunkGuard {
    chunk: Arc<Chunk>,
    releaser: Option<Arc<dyn ChunkReleaser>>,
}

impl ChunkGuard {
    /// create a new chunk guard with a release function
    pub fn new(chunk: Arc<Chunk>, releaser: Option<Arc<dyn ChunkReleaser>>) -> Self {
        Self { chunk, releaser }
    }

    /// get the chunk id
    pub fn chunk_id(&self) -> ChunkId {
        self.chunk.meta.chunk_id
    }
}

impl Deref for ChunkGuard {
    type Target = Chunk;

    fn deref(&self) -> &Self::Target {
        &self.chunk
    }
}

impl Drop for ChunkGuard {
    fn drop(&mut self) {
        if let Some(releaser) = self.releaser.take() {
            releaser.release(self.chunk_id())
        }
    }
}

/// cache metrics for monitoring
#[derive(Debug, Clone, Default)]
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub size: u64,
    pub capacity: u64,
}

/// trait for managing chunks in memory
#[async_trait]
pub trait ChunkManager: Send + Sync {
    /// get a chunk by id, loads from store if necessary.
    /// returns a guard that must be held while using the chunk.
    /// waits if cache is full and all chunks are in use.
    async fn get(&self, chunk_id: ChunkId) -> ChronosResult<ChunkGuard>;

    /// put a chunk. perists to store (write-through) and caches for future access.
    /// if the chunk already exists, it will be updated
    async fn put(&self, chunk: Chunk) -> ChronosResult<()>;

    /// signal that chunks may be needed soon (prefetch hint),
    /// loads chunks in background without acquiring references (ref_count = 0),
    /// does not block - spawns backgournd tasks for loading
    fn signal(&self, chunk_ids: &[ChunkId]);

    /// get current stat statistics
    fn stats(&self) -> CacheMetrics;
}

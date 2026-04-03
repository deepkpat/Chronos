use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::fs;

use crate::chunk::Chunk;
use crate::chunk_store::ChunkStore;
use crate::codec::ChunkCodec;
use crate::errors::{ChronosError, ChronosResult};
use crate::types::ChunkId;

/// filesystem-based chunk store
pub struct FileSystemChunkStore {
    base_path: PathBuf,
    codec: Arc<dyn ChunkCodec>,
}

impl FileSystemChunkStore {
    /// create a new filesystem chunk store
    pub fn new(base_path: PathBuf, codec: Arc<dyn ChunkCodec>) -> Self {
        Self { base_path, codec }
    }

    /// get the file path for a chunk
    fn chunk_path(&self, chunk_id: ChunkId) -> PathBuf {
        let filename = format!("{}.chunk", chunk_id);
        self.base_path.join(filename)
    }
}

#[async_trait]
impl ChunkStore for FileSystemChunkStore {
    async fn save(&self, chunk: &Chunk) -> ChronosResult<()> {
        let path = self.chunk_path(chunk.meta.chunk_id);

        // ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| ChronosError::IoError(format!("failed to create directory: {}", e)))?;
        }

        // encode chunk
        let bytes = self.codec.encode(chunk.clone())?;

        // write to file
        fs::write(&path, bytes)
            .await
            .map_err(|e| ChronosError::IoError(format!("failed to write chunk: {}", e)))?;

        Ok(())
    }

    async fn load(&self, chunk_id: ChunkId) -> ChronosResult<Option<Chunk>> {
        let path = self.chunk_path(chunk_id);

        if !path.exists() {
            return Ok(None);
        }

        let bytes = fs::read(&path)
            .await
            .map_err(|e| ChronosError::IoError(format!("failed to read chunk: {}", e)))?;

        self.codec.decode(&bytes)
    }

    async fn delete(&self, chunk_id: ChunkId) -> ChronosResult<()> {
        let path = self.chunk_path(chunk_id);

        if path.exists() {
            fs::remove_file(&path)
                .await
                .map_err(|e| ChronosError::IoError(format!("failed to delete chunk: {}", e)))?;
        }

        Ok(())
    }

    async fn exists(&self, chunk_id: ChunkId) -> ChronosResult<bool> {
        let path = self.chunk_path(chunk_id);
        Ok(path.exists())
    }
}

use crate::{chunk::Chunk, errors::ChronosResult};

pub trait ChunkCodec: Send + Sync {
    /// returns the name of the codec
    fn name(&self) -> &'static str;

    /// encode a chunk into bytes
    fn encode(&self, chunk: Chunk) -> ChronosResult<Vec<u8>>;

    /// decode bytes into a chunk
    fn decode(&self, bytes: &[u8]) -> ChronosResult<Option<Chunk>>;
}

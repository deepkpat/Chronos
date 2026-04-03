pub struct JsonCodec;

impl JsonCodec {
    pub fn new() -> Self {
        Self
    }
}

impl crate::codec::ChunkCodec for JsonCodec {
    fn name(&self) -> &'static str {
        "json"
    }

    fn encode(&self, chunk: crate::chunk::Chunk) -> crate::errors::ChronosResult<Vec<u8>> {
        serde_json::to_vec(&chunk)
            .map_err(|e| crate::errors::ChronosError::EncodeError(e.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> crate::errors::ChronosResult<Option<crate::chunk::Chunk>> {
        let chunk = serde_json::from_slice(bytes)
            .map_err(|e| crate::errors::ChronosError::DecodeError(e.to_string()))?;
        Ok(Some(chunk))
    }
}

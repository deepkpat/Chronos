use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::codec::ChunkCodec;
use crate::errors::{ChronosError, ChronosResult};

/// singleton registry for managing codec implementations
pub struct CodecRegistry {
    codecs: Mutex<HashMap<String, Arc<dyn ChunkCodec>>>,
}

impl CodecRegistry {
    /// create a new empty registry
    fn new() -> Self {
        Self {
            codecs: Mutex::new(HashMap::new()),
        }
    }

    /// get the singleton instance
    pub fn instance() -> Arc<Self> {
        static REGISTRY: std::sync::OnceLock<Arc<CodecRegistry>> = std::sync::OnceLock::new();
        REGISTRY
            .get_or_init(|| {
                let registry = Arc::new(Self::new());
                registry.register_all();
                registry
            })
            .clone()
    }

    /// register all available codecs
    fn register_all(&self) {
        let _ = self.register(crate::codec::JsonCodec::new());
    }

    /// register a codec with a given name
    pub fn register<C: ChunkCodec + 'static>(&self, codec: C) -> ChronosResult<()> {
        let mut codecs = self
            .codecs
            .lock()
            .map_err(|_| ChronosError::RegistryError("lock poisoned".to_string()))?;
        let name = codec.name().to_string();
        codecs.insert(name, Arc::new(codec));
        Ok(())
    }

    /// get a codec by name
    pub fn get(&self, name: &str) -> ChronosResult<Arc<dyn ChunkCodec>> {
        let codecs = self
            .codecs
            .lock()
            .map_err(|_| ChronosError::RegistryError("lock poisoned".to_string()))?;
        codecs
            .get(name)
            .cloned()
            .ok_or_else(|| ChronosError::CodecNotFound(name.to_string()))
    }

    /// list all registered codec names
    pub fn list(&self) -> ChronosResult<Vec<String>> {
        let codecs = self
            .codecs
            .lock()
            .map_err(|_| ChronosError::RegistryError("lock poisoned".to_string()))?;
        Ok(codecs.keys().cloned().collect())
    }

    /// check if a codec is registered
    pub fn contains(&self, name: &str) -> ChronosResult<bool> {
        let codecs = self
            .codecs
            .lock()
            .map_err(|_| ChronosError::RegistryError("lock poisoned".to_string()))?;
        Ok(codecs.contains_key(name))
    }
}

impl Default for CodecRegistry {
    fn default() -> Self {
        Self::new()
    }
}

use thiserror::Error;

/// custom error types for chronos
#[derive(Error, Debug, Clone)]
pub enum ChronosError {
    #[error("an error occured: {0}")]
    Err(String),

    #[error("encode error: {0}")]
    EncodeError(String),

    #[error("decode error: {0}")]
    DecodeError(String),

    #[error("codec not found: {0}")]
    CodecNotFound(String),

    #[error("registry error: {0}")]
    RegistryError(String),

    #[error("io error: {0}")]
    IoError(String),
}

/// result type alias
pub type ChronosResult<T> = Result<T, ChronosError>;

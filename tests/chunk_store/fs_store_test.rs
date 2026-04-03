use std::sync::Arc;

use uuid::Uuid;

use chronos::chunk::{Chunk, ChunkData, ChunkMeta};
use chronos::chunk_store::{ChunkStore, FileSystemChunkStore};
use chronos::codec::JsonCodec;
use chronos::types::{ChunkId, SeriesId};

fn new_chunk_id() -> ChunkId {
    Uuid::new_v4()
}

fn new_series_id() -> SeriesId {
    Uuid::new_v4()
}

fn create_test_chunk(chunk_id: ChunkId, series_id: SeriesId) -> Chunk {
    Chunk {
        meta: ChunkMeta {
            chunk_id,
            series_id,
            start_time: 1000,
            end_time: 2000,
        },
        data: ChunkData {
            timestamps: vec![1000, 1500, 2000],
            values: vec![1.0, 2.0, 3.0],
        },
    }
}

#[tokio::test]
async fn test_save_and_load() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store =
        FileSystemChunkStore::new(temp_dir.path().to_path_buf(), Arc::new(JsonCodec::new()));

    let chunk_id = new_chunk_id();
    let series_id = new_series_id();
    let chunk = create_test_chunk(chunk_id, series_id);

    // save the chunk
    store.save(&chunk).await.unwrap();

    // load the chunk
    let loaded = store.load(chunk_id).await.unwrap();
    assert!(loaded.is_some());

    let loaded = loaded.unwrap();
    assert_eq!(loaded.meta.chunk_id, chunk_id);
    assert_eq!(loaded.meta.series_id, series_id);
    assert_eq!(loaded.data.timestamps, chunk.data.timestamps);
    assert_eq!(loaded.data.values, chunk.data.values);
}

#[tokio::test]
async fn test_load_nonexistent() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store =
        FileSystemChunkStore::new(temp_dir.path().to_path_buf(), Arc::new(JsonCodec::new()));

    let chunk_id = new_chunk_id();
    let loaded = store.load(chunk_id).await.unwrap();

    assert!(loaded.is_none());
}

#[tokio::test]
async fn test_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store =
        FileSystemChunkStore::new(temp_dir.path().to_path_buf(), Arc::new(JsonCodec::new()));

    let chunk_id = new_chunk_id();
    let series_id = new_series_id();
    let chunk = create_test_chunk(chunk_id, series_id);

    // save the chunk
    store.save(&chunk).await.unwrap();

    // verify it exists
    assert!(store.exists(chunk_id).await.unwrap());

    // delete the chunk
    store.delete(chunk_id).await.unwrap();

    // verify it's gone
    assert!(!store.exists(chunk_id).await.unwrap());
}

#[tokio::test]
async fn test_delete_nonexistent() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store =
        FileSystemChunkStore::new(temp_dir.path().to_path_buf(), Arc::new(JsonCodec::new()));

    let chunk_id = new_chunk_id();

    // deleting nonexistent chunk should not error
    store.delete(chunk_id).await.unwrap();
}

#[tokio::test]
async fn test_exists() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store =
        FileSystemChunkStore::new(temp_dir.path().to_path_buf(), Arc::new(JsonCodec::new()));

    let chunk_id = new_chunk_id();
    let series_id = new_series_id();

    // initially should not exist
    assert!(!store.exists(chunk_id).await.unwrap());

    // save the chunk
    let chunk = create_test_chunk(chunk_id, series_id);
    store.save(&chunk).await.unwrap();

    // now should exist
    assert!(store.exists(chunk_id).await.unwrap());
}

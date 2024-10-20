use std::sync::Arc;

use crate::common::chunk;
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::DynObjectStore;
use reqwest::Url;

/// A client for interacting with an object store.
/// 
/// This exists as a separate type, so we can build it _once_
/// and then re-use it in the producer/consumer for all communication going forward from there.
#[derive(Debug, Clone)]
pub struct ObjectStoreClient {
    object_store: Arc<DynObjectStore>,
    base_path: Path,
}

impl ObjectStoreClient {
    /// Creates a new client for interacting with an object store.
    /// 
    /// The given `object_store_url` recognizes the formats detailed here: https://docs.rs/object_store/0.11.1/object_store/enum.ObjectStoreScheme.html#method.parse
    /// Most importantly, we support GCS (for production usage) and local file systems (for testing).
    pub fn new(object_store_url: &str) -> anyhow::Result<Self> {
        let url = Url::parse(object_store_url)?;
        let (object_store, base_path) = object_store::parse_url(&url)?;
        Ok(ObjectStoreClient {object_store: Arc::new(object_store), base_path: base_path })
    }

    pub async fn store_chunks(&self, chunk_contents: impl TryStreamExt<Ok = chunk::Content, Error = anyhow::Error>) -> anyhow::Result<u32> {
        chunk_contents.try_fold(0, |chunk_index, chunk_content| async move {
            match chunk_content {
                None => {},
                Some(content) => {
                    let path = Path::from(format!("{}/{}", self.base_path, chunk_index));
                    self.object_store.put(&path, content.into()).await?;
                }
            }
            Ok(chunk_index + 1)
        }).await
    }

    pub async fn retrieve_chunk(&self, chunk_index: chunk::ChunkIndex) -> anyhow::Result<Bytes> {
        let bytes = self.object_store.get(&self.chunk_path(chunk_index)).await?.bytes().await?;
        Ok(bytes)
    }

    pub async fn retrieve_chunks(&self, chunk_count: i64) -> impl TryStreamExt<Ok = Bytes, Error = anyhow::Error> + '_ {
        stream::iter(0..chunk_count).then(move |chunk_index| async move {
            self.retrieve_chunk(chunk_index.into()).await
        })
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    fn chunk_path(&self, chunk_index: chunk::ChunkIndex) -> Path {
        Path::from(format!("{}/{}", self.base_path, chunk_index))
    }

}

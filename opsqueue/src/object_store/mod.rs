use std::sync::Arc;

use crate::common::chunk;
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
    pub url: Box<str>,
    object_store: Arc<DynObjectStore>,
    base_path: Path,
}

/// The object store doesn't really care whether the chunk contents sent to it
/// are 'input' (producer -> consumer) or 'output' (consumer -> producer),
/// but it has to be able to read/write both and disambiguate between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkType {
    /// Input chunk,
    /// data made by the producer and operated on by the consumer.
    Input,
    /// Output chunk, AKA 'chunk result',
    /// the outcome that is made by the consumer and returned to the producer
    Output,
}

impl std::fmt::Display for ChunkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkType::Input => write!(f, "in"),
            ChunkType::Output => write!(f, "out"),
        }
    }
}

impl ObjectStoreClient {
    /// Creates a new client for interacting with an object store.
    ///
    /// The given `object_store_url` recognizes the formats detailed here: https://docs.rs/object_store/0.11.1/object_store/enum.ObjectStoreScheme.html#method.parse
    /// Most importantly, we support GCS (for production usage) and local file systems (for testing).
    pub fn new(object_store_url: &str) -> anyhow::Result<Self> {
        let url = Url::parse(object_store_url)?;
        let (object_store, base_path) = object_store::parse_url(&url)?;
        Ok(ObjectStoreClient {url: object_store_url.into(), object_store: Arc::new(object_store), base_path })
    }

    pub async fn store_chunks(&self, submission_prefix: &str, chunk_type: ChunkType, chunk_contents: impl TryStreamExt<Ok = Vec<u8>, Error = anyhow::Error>) -> anyhow::Result<i64> {
        let chunk_count = chunk_contents.try_fold(0, |chunk_index, chunk_content| async move {
            self.store_chunk(submission_prefix, chunk_index.into(), chunk_type, chunk_content).await?;
            tracing::debug!("Upladed chunk {}", self.chunk_path(submission_prefix, chunk_index.into(), chunk_type));
            Ok(chunk_index + 1)
        }).await?;
        tracing::debug!("Finished uploading all {} chunks for submission prefix {}", chunk_count, submission_prefix);
        Ok(chunk_count)
    }

    pub async fn store_chunk(&self, submission_prefix: &str, chunk_index: chunk::ChunkIndex, chunk_type: ChunkType, content: Vec<u8>) -> anyhow::Result<()> {
        let path = self.chunk_path(submission_prefix, chunk_index, chunk_type);
        self.object_store.put(&path, content.into()).await?;
        Ok(())
    }

    pub async fn retrieve_chunk(&self, submission_prefix: &str, chunk_index: chunk::ChunkIndex, chunk_type: ChunkType) -> anyhow::Result<Vec<u8>> {
        let bytes = self.object_store.get(&self.chunk_path(submission_prefix, chunk_index, chunk_type)).await?.bytes().await?.into();
        Ok(bytes)
    }

    pub async fn retrieve_chunks<'a>(&'a self, submission_prefix: &'a str, chunk_count: i64, chunk_type: ChunkType) -> impl TryStreamExt<Ok = Vec<u8>, Error = anyhow::Error> + 'a {
        stream::iter(0..chunk_count).then(move |chunk_index| async move {
            self.retrieve_chunk(submission_prefix, chunk_index.into(), chunk_type).await
        })
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    fn chunk_path(&self, submission_prefix: &str, chunk_index: chunk::ChunkIndex, chunk_type: ChunkType) -> Path {
        Path::from(format!("{}/{}/{}-{}.bin", self.base_path, submission_prefix, chunk_index, chunk_type))
    }
}

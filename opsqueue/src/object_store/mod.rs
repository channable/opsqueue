use std::sync::Arc;

use crate::common::chunk;
use futures::stream::{self, TryStreamExt};
use object_store::path::Path;
use object_store::DynObjectStore;
use reqwest::Url;
use ux_serde::u63;

/// A client for interacting with an object store.
///
/// This exists as a separate type, so we can build it _once_
/// and then re-use it in the producer/consumer for all communication going forward from there.
///
/// It is Arc-wrapped, allowing for cheap cloning
/// (which is especially necessary for `ObjectStoreClient::retrieve_chunks`)
#[derive(Debug, Clone)]
pub struct ObjectStoreClient(Arc<ObjectStoreClientInner>);

#[derive(Debug)]
pub struct ObjectStoreClientInner {
    url: Box<str>,
    object_store: Box<DynObjectStore>,
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

#[derive(thiserror::Error, Debug)]
pub enum ChunkRetrievalError {
    #[error("Failed to retrieve chunk ({submission_prefix}, {chunk_index}, {chunk_type}) from object store: {source}")]
    ObjectStoreError {
        source: object_store::Error,
        submission_prefix: Box<str>,
        chunk_index: chunk::ChunkIndex,
        chunk_type: ChunkType,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum ChunkStorageError {
    #[error("Failed to store chunk ({submission_prefix}, {chunk_index}, {chunk_type}) to object store: {source}")]
    ObjectStoreError {
        source: object_store::Error,
        submission_prefix: Box<str>,
        chunk_index: chunk::ChunkIndex,
        chunk_type: ChunkType,
    },
    #[error("Failed to read chunk element from stream/iterator at index {chunk_index}: ")]
    ChunkContentsEvalError {
        submission_prefix: Box<str>,
        chunk_index: chunk::ChunkIndex,
        chunk_type: ChunkType,
        source: anyhow::Error,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum ChunksStorageError {
    #[error(transparent)]
    ChunkStorageError(#[from] ChunkStorageError),
    #[error("Failed to read chunk element from stream/iterator: {source}")]
    ChunkContentsEvalError {
        submission_prefix: Box<str>,
        chunk_type: ChunkType,
        source: anyhow::Error,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum NewObjectStoreClientError {
    #[error("Failed to parse URL: {0}")]
    UrlParseFailure(#[from] url::ParseError),
    #[error("URL is not valid object store URL {0}")]
    ObjectStoreUrlFailure(#[from] object_store::Error),
}

impl ObjectStoreClient {
    /// Creates a new client for interacting with an object store.
    ///
    /// The given `object_store_url` recognizes the formats detailed here: https://docs.rs/object_store/0.11.1/object_store/enum.ObjectStoreScheme.html#method.parse
    /// Most importantly, we support GCS (for production usage) and local file systems (for testing).
    pub fn new(
        object_store_url: &str,
        options: Vec<(String, String)>,
    ) -> Result<Self, NewObjectStoreClientError> {
        let url = Url::parse(object_store_url)?;
        let (object_store, base_path) = object_store::parse_url_opts(&url, options)?;
        Ok(ObjectStoreClient(Arc::new(ObjectStoreClientInner {
            url: object_store_url.into(),
            object_store,
            base_path,
        })))
    }

    pub async fn store_chunks(
        &self,
        submission_prefix: &str,
        chunk_type: ChunkType,
        chunk_contents: impl TryStreamExt<Ok = Vec<u8>, Error = anyhow::Error>,
    ) -> Result<u63, ChunksStorageError> {
        use ChunksStorageError::*;
        let chunk_count = chunk_contents
            .try_fold(u63::new(0), |chunk_index, chunk_content| async move {
                self.store_chunk(
                    submission_prefix,
                    chunk_index.into(),
                    chunk_type,
                    chunk_content,
                )
                .await?;
                tracing::debug!(
                    "Upladed chunk {}",
                    self.chunk_path(submission_prefix, chunk_index.into(), chunk_type)
                );
                Ok(chunk_index + u63::new(1))
            })
            .await
            .map_err(|e| ChunkContentsEvalError {
                source: e,
                submission_prefix: submission_prefix.into(),
                chunk_type,
            })?;
        tracing::debug!(
            "Finished uploading all {} chunks for submission prefix {}",
            chunk_count,
            submission_prefix
        );
        Ok(chunk_count)
    }

    pub async fn store_chunk(
        &self,
        submission_prefix: &str,
        chunk_index: chunk::ChunkIndex,
        chunk_type: ChunkType,
        content: Vec<u8>,
    ) -> Result<(), ChunkStorageError> {
        use ChunkStorageError::*;
        let path = self.chunk_path(submission_prefix, chunk_index, chunk_type);
        self.0
            .object_store
            .put(&path, content.into())
            .await
            .map_err(|e| ObjectStoreError {
                source: e,
                submission_prefix: submission_prefix.into(),
                chunk_index,
                chunk_type,
            })?;
        Ok(())
    }

    pub async fn retrieve_chunk(
        &self,
        submission_prefix: &str,
        chunk_index: chunk::ChunkIndex,
        chunk_type: ChunkType,
    ) -> Result<Vec<u8>, ChunkRetrievalError> {
        use ChunkRetrievalError::*;
        let res = async move {
            let bytes = self
                .0
                .object_store
                .get(&self.chunk_path(submission_prefix, chunk_index, chunk_type))
                .await?
                .bytes()
                .await?
                .into();
            Ok(bytes)
        }
        .await;
        res.map_err(|e| ObjectStoreError {
            source: e,
            submission_prefix: submission_prefix.into(),
            chunk_index,
            chunk_type,
        })
    }
    pub async fn retrieve_chunks<Prefix: Into<String>>(
        &self,
        submission_prefix: Prefix,
        chunk_count: u63,
        chunk_type: ChunkType,
    ) -> impl TryStreamExt<Ok = Vec<u8>, Error = ChunkRetrievalError> + 'static {
        let submission_prefix: String = submission_prefix.into();
        let initial_state = (self.clone(), submission_prefix, u63::new(0));
        stream::unfold(initial_state, move |(client, prefix, index)| async move {
            if index >= chunk_count {
                return None;
            }
            let element = client
                .retrieve_chunk(&prefix, index.into(), chunk_type)
                .await;
            let new_state = (client, prefix, index + u63::new(1));

            Some((element, new_state))
        })
    }

    pub fn base_path(&self) -> &Path {
        &self.0.base_path
    }

    fn chunk_path(
        &self,
        submission_prefix: &str,
        chunk_index: chunk::ChunkIndex,
        chunk_type: ChunkType,
    ) -> Path {
        Path::from(format!(
            "{}/{}/{}-{}.bin",
            self.0.base_path, submission_prefix, chunk_index, chunk_type
        ))
    }

    pub fn url(&self) -> &str {
        &self.0.url
    }
}

// #[must_use("Streams do nothig unless polled")]
// struct MyStream {
//     object_store_client: ObjectStoreClient,
//     submission_prefix: String,
//     range: std::ops::Range<u63>,
//     chunk_type: ChunkType,
// }

// impl futures::Stream for MyStream {
//     type Item = Result<Vec<u8>, ChunkRetrievalError>;

//     fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
//         let next_elem = self.range.start.into();
//         let fut = self.object_store_client.retrieve_chunk(&self.submission_prefix, next_elem, self.chunk_type);
//         tokio::pin!(fut);
//         match fut.poll_unpin() {
//             Poll::Pending => Poll::Pending,
//             Poll::Ready(res) => Poll::Ready(Some(res)),
//         }
//     }

//     fn size_hint(&self) -> (usize, Option<usize>) {
//         let start: u64 = self.range.start.into();
//         let end: u64 = self.range.end.into();
//         (start as usize, Some(end as usize))
//     }
// }

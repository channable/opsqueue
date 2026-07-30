use std::sync::Arc;

use crate::common::chunk;
use futures::stream::{self, TryStreamExt};
use object_store::DynObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use reqwest::Url;
use ux::u63;

// Re-export the `object_store` types that appear in this module's public API
// (`new_with_retry`'s parameters and `ChunkRetrievalError`/`ChunkStorageError`'s
// `source` field). Downstream crates should name these through opsqueue instead
// of taking their own direct dependency on the `object_store` crate, which would
// have to be kept version-locked with the one opsqueue links against.
pub use object_store::{BackoffConfig, Error, RetryConfig};

/// A client for interacting with an object store.
///
/// This exists as a separate type, so we can build it _once_
/// and then re-use it in the producer/consumer for all communication going forward from there.
///
/// It is Arc-wrapped, allowing for cheap cloning
/// (which is especially necessary for [`ObjectStoreClient::retrieve_chunks`])
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
    #[error(
        "Failed to retrieve chunk ({submission_prefix}, {chunk_index}, {chunk_type}) from object store"
    )]
    ObjectStoreError {
        #[source]
        source: object_store::Error,
        submission_prefix: Box<str>,
        chunk_index: chunk::ChunkIndex,
        chunk_type: ChunkType,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum ChunkStorageError {
    #[error(
        "Failed to store chunk ({submission_prefix}, {chunk_index}, {chunk_type}) to object store"
    )]
    ObjectStoreError {
        #[source]
        source: object_store::Error,
        submission_prefix: Box<str>,
        chunk_index: chunk::ChunkIndex,
        chunk_type: ChunkType,
    },
    #[error("Failed to read chunk element from stream/iterator at index {chunk_index}")]
    ChunkContentsEvalError {
        submission_prefix: Box<str>,
        chunk_index: chunk::ChunkIndex,
        chunk_type: ChunkType,
        #[source]
        source: anyhow::Error,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum ChunksStorageError {
    #[error(transparent)]
    ChunkStorageError(#[from] ChunkStorageError),
    #[error("Failed to read chunk element from stream/iterator")]
    ChunkContentsEvalError {
        submission_prefix: Box<str>,
        chunk_type: ChunkType,
        #[source]
        source: anyhow::Error,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum NewObjectStoreClientError {
    #[error("Failed to parse URL")]
    UrlParseFailure(#[from] url::ParseError),
    #[error("URL is not valid object store URL")]
    ObjectStoreUrlFailure(#[from] object_store::Error),
}

impl ObjectStoreClient {
    /// Creates a new client for interacting with an object store.
    ///
    /// The given `object_store_url` recognizes the formats detailed [here](https://docs.rs/object_store/0.11.1/object_store/enum.ObjectStoreScheme.html#method.parse).
    /// Most importantly, we support GCS (for production usage) and local file systems (for testing).
    ///
    /// Uses the object store's default transport-level retry configuration; use
    /// [`ObjectStoreClient::new_with_retry`] to override it.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL cannot be parsed or if object store initialization fails.
    pub fn new(
        object_store_url: &str,
        options: Vec<(String, String)>,
    ) -> Result<Self, NewObjectStoreClientError> {
        Self::new_with_retry(object_store_url, options, RetryConfig::default())
    }

    /// Like [`ObjectStoreClient::new`] but overrides the transport-level
    /// retry configuration used by the underlying object store client.
    ///
    /// The retry configuration is applied for backends whose builder exposes
    /// [`with_retry`](object_store::gcp::GoogleCloudStorageBuilder::with_retry)
    /// (currently GCS and HTTP). For local and in-memory backends the retry
    /// config has no effect and is silently ignored, matching the behaviour
    /// of [`ObjectStoreClient::new`].
    ///
    /// # Errors
    ///
    /// Returns an error if the URL cannot be parsed or if object store
    /// initialization fails.
    pub fn new_with_retry(
        object_store_url: &str,
        options: Vec<(String, String)>,
        retry_config: RetryConfig,
    ) -> Result<Self, NewObjectStoreClientError> {
        use object_store::ClientConfigKey;
        use object_store::ObjectStoreScheme;
        use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
        use object_store::http::HttpBuilder;
        use std::str::FromStr;

        let url = Url::parse(object_store_url)?;
        let (scheme, raw_path) =
            ObjectStoreScheme::parse(&url).map_err(object_store::Error::from)?;
        let base_path = Path::parse(raw_path).map_err(object_store::Error::from)?;

        let object_store: Box<DynObjectStore> = match scheme {
            ObjectStoreScheme::GoogleCloudStorage => {
                let builder = options.into_iter().fold(
                    GoogleCloudStorageBuilder::new()
                        .with_url(object_store_url.to_string())
                        .with_retry(retry_config),
                    |builder, (key, value)| match GoogleConfigKey::from_str(
                        &key.to_ascii_lowercase(),
                    ) {
                        Ok(config_key) => builder.with_config(config_key, value),
                        Err(_) => builder,
                    },
                );
                Box::new(builder.build()?)
            }
            ObjectStoreScheme::Http => {
                let url_without_path = &url[..url::Position::BeforePath];
                let builder = options.into_iter().fold(
                    HttpBuilder::new()
                        .with_url(url_without_path)
                        .with_retry(retry_config),
                    |builder, (key, value)| match ClientConfigKey::from_str(
                        &key.to_ascii_lowercase(),
                    ) {
                        Ok(config_key) => builder.with_config(config_key, value),
                        Err(_) => builder,
                    },
                );
                Box::new(builder.build()?)
            }
            _ => {
                // Retry configuration is not meaningful for local/in-memory
                // backends; fall back to the default construction path so
                // that the `options` list is still honoured for those cases.
                let (store, _) = object_store::parse_url_opts(&url, options)?;
                store
            }
        };

        Ok(ObjectStoreClient(Arc::new(ObjectStoreClientInner {
            url: object_store_url.into(),
            object_store,
            base_path,
        })))
    }

    /// Store a stream of chunks and return the number of stored chunks.
    ///
    /// # Errors
    ///
    /// Returns an error if evaluating the stream or uploading any chunk fails.
    pub async fn store_chunks(
        &self,
        submission_prefix: &str,
        chunk_type: ChunkType,
        chunk_contents: impl TryStreamExt<Ok = Vec<u8>, Error = anyhow::Error>,
    ) -> Result<u63, ChunksStorageError> {
        use ChunksStorageError::ChunkContentsEvalError;
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
                    "Uploaded chunk {}",
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

    /// Store one chunk in object storage.
    ///
    /// # Errors
    ///
    /// Returns an error if uploading fails.
    pub async fn store_chunk(
        &self,
        submission_prefix: &str,
        chunk_index: chunk::ChunkIndex,
        chunk_type: ChunkType,
        content: Vec<u8>,
    ) -> Result<(), ChunkStorageError> {
        use ChunkStorageError::ObjectStoreError;
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

    /// Retrieve one chunk from object storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the object cannot be read.
    pub async fn retrieve_chunk(
        &self,
        submission_prefix: &str,
        chunk_index: chunk::ChunkIndex,
        chunk_type: ChunkType,
    ) -> Result<Vec<u8>, ChunkRetrievalError> {
        use ChunkRetrievalError::ObjectStoreError;
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
    pub fn retrieve_chunks<Prefix: Into<String>>(
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

    #[must_use]
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

    #[must_use]
    pub fn url(&self) -> &str {
        &self.0.url
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::chunk::ChunkIndex;
    use object_store::{BackoffConfig, RetryConfig};
    use std::time::Duration;
    use ux::u63;

    fn tight_retry_config() -> RetryConfig {
        RetryConfig {
            backoff: BackoffConfig {
                init_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(2),
                base: 2.0,
            },
            max_retries: 3,
            retry_timeout: Duration::from_secs(1),
        }
    }

    #[tokio::test]
    async fn new_with_retry_supports_in_memory_backend() {
        // Non-cloud backends must keep working: `new_with_retry` should build
        // successfully and behave identically to `new` for `memory://` URLs.
        let client =
            ObjectStoreClient::new_with_retry("memory:///", Vec::new(), tight_retry_config())
                .expect("memory:/// URL should build with new_with_retry");

        let chunk_index: ChunkIndex = u63::new(0).into();
        client
            .store_chunk("test-prefix", chunk_index, ChunkType::Input, b"hi".to_vec())
            .await
            .expect("store on in-memory backend should succeed");

        let retrieved = client
            .retrieve_chunk("test-prefix", chunk_index, ChunkType::Input)
            .await
            .expect("retrieve on in-memory backend should succeed");
        assert_eq!(retrieved, b"hi");
    }

    #[test]
    fn new_with_retry_rejects_malformed_url() {
        // Regression guard: bad URLs must still surface as
        // `UrlParseFailure` and not, e.g., panic while classifying the
        // scheme.
        let err = ObjectStoreClient::new_with_retry("not a url", Vec::new(), tight_retry_config())
            .expect_err("malformed URL should not build");
        assert!(
            matches!(err, NewObjectStoreClientError::UrlParseFailure(_)),
            "unexpected error variant: {err:?}"
        );
    }

    #[test]
    fn new_with_retry_builds_http_backend_without_network_io() {
        // The HTTP arm of `new_with_retry` goes through `HttpBuilder`, which
        // is a different code path from the `parse_url_opts` fallback covered
        // by the memory:// and file:// tests. Building the client does not
        // touch the network, so we can point it at an unreachable localhost
        // port and still assert that construction succeeds.
        let client = ObjectStoreClient::new_with_retry(
            "http://127.0.0.1:1/some/prefix",
            Vec::new(),
            tight_retry_config(),
        )
        .expect("http:// URL should build with new_with_retry");
        // Sanity-check the base path was captured (the URL segment after the
        // authority).
        assert!(format!("{client:?}").contains("some/prefix"));
    }

    /// A minimal service account JSON that carries `disable_oauth: true`, so
    /// `GoogleCloudStorageBuilder::build()` succeeds without talking to any
    /// external auth service. Mirrors the fake key used inside `object_store`'s
    /// own test suite.
    const FAKE_GCS_SERVICE_ACCOUNT_KEY: &str = r#"{
        "private_key": "private_key",
        "private_key_id": "private_key_id",
        "client_email": "client_email",
        "disable_oauth": true
    }"#;

    #[test]
    fn new_with_retry_builds_gcs_backend_with_disabled_oauth() {
        // The GCS arm of `new_with_retry` goes through `GoogleCloudStorageBuilder`,
        // which is the branch that motivated this whole change (issue #3883).
        // We feed it a service-account JSON with `disable_oauth: true` so the
        // builder skips real authentication and `build()` completes without
        // network I/O.
        let options = vec![(
            "service_account_key".to_string(),
            FAKE_GCS_SERVICE_ACCOUNT_KEY.to_string(),
        )];
        let client = ObjectStoreClient::new_with_retry(
            "gs://some-bucket/some/prefix",
            options,
            tight_retry_config(),
        )
        .expect("gs:// URL should build with new_with_retry");
        assert!(format!("{client:?}").contains("some/prefix"));
    }

    #[test]
    fn new_with_retry_forwards_options_to_gcs_builder_and_ignores_unknown_keys() {
        // Regression guard for the `options.into_iter().fold(...)` in the GCS
        // arm: if a future refactor stops forwarding options to the builder,
        // `service_account_key` below would no longer disable OAuth and
        // `build()` would fall back to ADC lookup — which we can't rely on in
        // CI. If instead the fold turned unknown keys into hard errors,
        // `totally_unknown_key` would fail the build. Both directions of
        // regression are caught by asserting `Ok(_)` here.
        let options = vec![
            (
                "service_account_key".to_string(),
                FAKE_GCS_SERVICE_ACCOUNT_KEY.to_string(),
            ),
            ("totally_unknown_key".to_string(), "some-value".to_string()),
        ];
        ObjectStoreClient::new_with_retry("gs://some-bucket/", options, tight_retry_config())
            .expect("gs:// URL with mixed known+unknown options should build");
    }
}

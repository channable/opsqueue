use std::time::Duration;

use backon::BackoffBuilder;
use backon::FibonacciBuilder;
use backon::Retryable;

use crate::common::submission::{SubmissionId, SubmissionStatus};
use crate::tracing::CarrierMap;

use super::common::InsertSubmission;

fn retry_policy() -> impl BackoffBuilder {
    FibonacciBuilder::default()
        .with_jitter()
        .with_min_delay(Duration::from_millis(10))
        .with_max_delay(Duration::from_secs(5))
        .with_max_times(100)
}

/// A producer's interface to the server.
///
/// This client sends HTTP requests to the opsqueue server, and retries on errors where
/// it makes sense to retry them, such as when the request times out.
#[derive(Debug, Clone)]
pub struct Client {
    base_url: Box<str>,
    http_client: reqwest::Client,
}

impl Client {
    /// Construct a new producer client.
    ///
    /// `host` is where the `/producer/...` endpoints can be reached over http.
    /// You can include the scheme if it's `http://` or `https://`. If it's not
    /// included, http is chosen by default. If it's not supported, this function
    /// panics.
    ///
    /// Examples: `0.0.0.0:1312`, `my.opsqueue.instance.example.com`, `https://services.example.com/opsqueue`
    pub fn new(host: &str) -> Self {
        let http_client = reqwest::Client::new();
        let base_url = match host.split_once("://") {
            Some(("http" | "https", _)) => format!("{host}/producer"),
            None => format!("http://{host}/producer"),
            Some((scheme, _)) => panic!("Unsupported scheme: {scheme}, must be 'http' or 'https'"),
        };
        Client {
            base_url: base_url.into_boxed_str(),
            http_client,
        }
    }
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
    /// Get the total number of non-failed, non-completed submissions currently
    /// known to the server.
    ///
    /// This uses the `/producer/submissions/count` endpoint.
    pub async fn count_submissions(&self) -> Result<u32, InternalProducerClientError> {
        (|| async {
            let base_url = &self.base_url;
            let resp = self
                .http_client
                .get(format!("{base_url}/submissions/count"))
                .send()
                .await?;
            let bytes = resp.bytes().await?;
            let body = serde_json::from_slice(&bytes)?;

            Ok(body)
        })
        .retry(retry_policy())
        .when(InternalProducerClientError::is_ephemeral)
        .notify(|err, dur| {
            tracing::debug!("retrying error {err:?} with sleeping {dur:?}");
        })
        .await
    }

    /// Create a new submission for consumers to pick up.
    ///
    /// This uses the POST `/producer/submissions` endpoint.
    pub async fn insert_submission(
        &self,
        submission: &InsertSubmission,
        otel_trace_carrier: &CarrierMap,
    ) -> Result<SubmissionId, InternalProducerClientError> {
        (|| async {
            let base_url = &self.base_url;
            let resp = self
                .http_client
                .post(format!("{base_url}/submissions"))
                .headers(otel_trace_carrier.try_into().unwrap_or_default())
                .json(submission)
                .send()
                .await?;
            let bytes = resp.bytes().await?;
            let body = serde_json::from_slice(&bytes)?;
            Ok(body)
        })
        .retry(retry_policy())
        .when(InternalProducerClientError::is_ephemeral)
        .notify(|err, dur| {
            tracing::debug!("retrying error {err:?} with sleeping {dur:?}");
        })
        .await
    }

    /// Get the status of an existing submission identified by its `submission_id`.
    ///
    /// This uses the GET `/producer/submissions` endpoint.
    pub async fn get_submission(
        &self,
        submission_id: SubmissionId,
    ) -> Result<Option<SubmissionStatus>, InternalProducerClientError> {
        (|| async {
            let base_url = &self.base_url;
            let resp = self
                .http_client
                .get(format!("{base_url}/submissions/{submission_id}"))
                .send()
                .await?;
            let bytes = resp.bytes().await?;
            let body = serde_json::from_slice(&bytes)?;
            Ok(body)
        })
        .retry(retry_policy())
        .when(InternalProducerClientError::is_ephemeral)
        .notify(|err, dur| {
            tracing::debug!("retrying error {err:?} with sleeping {dur:?}");
        })
        .await
    }

    pub async fn lookup_submission_id_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Option<SubmissionId>, InternalProducerClientError> {
        (|| async {
            let base_url = &self.base_url;
            let resp = self
                .http_client
                .get(format!(
                    "{base_url}/submissions/lookup_id_by_prefix/{prefix}"
                ))
                .send()
                .await?;
            let bytes = resp.bytes().await?;
            let body = serde_json::from_slice(&bytes)?;
            Ok(body)
        })
        .retry(retry_policy())
        .when(InternalProducerClientError::is_ephemeral)
        .notify(|err, dur| {
            tracing::debug!("retrying error {err:?} with sleeping {dur:?}");
        })
        .await
    }

    /// Get the server's version from the `/version` endpoint.
    ///
    /// The result will be the value of [`VERSION_CARGO_SEMVER`][crate::VERSION_CARGO_SEMVER]
    /// prefixed with a "v", for example `v0.30.5`.
    pub async fn server_version(&self) -> Result<String, InternalProducerClientError> {
        let base_url = &self.base_url;
        let resp = self
            .http_client
            .get(format!("{base_url}/version"))
            .send()
            .await?;
        let text = resp.text().await?;
        Ok(text)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum InternalProducerClientError {
    #[error("HTTP request failed: {0}")]
    HTTPClientError(#[from] reqwest::Error),
    #[error("Error decoding JSON response: {0}")]
    ResponseDecodingError(#[from] serde_json::Error),
}

impl InternalProducerClientError {
    pub fn is_ephemeral(&self) -> bool {
        match self {
            // NOTE: In the case of an ungraceful restart, this case might theoretically trigger.
            // So even cleaner would be a tiny retry loop for this special case.
            // However, we certainly **do not** want to wait multiple minutes before returning.
            Self::ResponseDecodingError(_) => false,
            // NOTE: reqwest doesn't make this very easy as it has a single error typed used for _everything_
            // Maybe a different HTTP client library is nicer in this regard?
            Self::HTTPClientError(inner) => {
                inner.is_connect() || inner.is_timeout() || inner.is_decode()
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
mod tests {
    use crate::{
        common::{
            chunk::ChunkSize,
            submission::{self, SubmissionStatus},
            StrategicMetadataMap,
        },
        db::{DBPools, WriterPool},
        producer::common::ChunkContents,
    };

    use super::*;

    async fn start_server_in_background(pool: &sqlx::SqlitePool, url: &str) {
        let db_pools = DBPools::from_test_pool(pool);
        // We spawn the separate server
        // and immediately yield.
        // This to ensure that the new server task has a chance to run
        // before continuing on the single-threaded tokio test runtime
        tokio::spawn(super::super::server::serve_for_tests(db_pools, url.into()));
        tokio::task::yield_now().await;
    }

    #[sqlx::test]
    async fn test_count_submissions(pool: sqlx::SqlitePool) {
        let url = "0.0.0.0:4002";
        start_server_in_background(&pool, url).await;
        let client = Client::new(url);

        let count = client.count_submissions().await.expect("Should be OK");
        assert_eq!(count, 0);

        let pool = WriterPool::new(pool);
        let mut conn = pool.writer_conn().await.unwrap();
        submission::db::insert_submission_from_chunks(
            None,
            vec![None, None, None],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            &mut conn,
        )
        .await
        .expect("Insertion failed");

        let count = client.count_submissions().await.expect("Should be OK");
        assert_eq!(count, 1);
    }

    #[sqlx::test]
    async fn test_insert_submission(pool: sqlx::SqlitePool) {
        let url = "0.0.0.0:4000";
        start_server_in_background(&pool, url).await;
        let client = Client::new(url);

        let pool = WriterPool::new(pool);
        let mut conn = pool.writer_conn().await.unwrap();
        let count = submission::db::count_submissions(&mut conn)
            .await
            .expect("Should be OK");
        assert_eq!(count, 0);

        let submission = InsertSubmission {
            chunk_contents: ChunkContents::Direct {
                contents: vec![None, None, None],
            },
            metadata: None,
            strategic_metadata: Default::default(),
            chunk_size: None,
        };
        client
            .insert_submission(&submission, &Default::default())
            .await
            .expect("Should be OK");

        let count = submission::db::count_submissions(&mut conn)
            .await
            .expect("Should be OK");
        assert_eq!(count, 1);

        client
            .insert_submission(&submission, &Default::default())
            .await
            .expect("Should be OK");
        client
            .insert_submission(&submission, &Default::default())
            .await
            .expect("Should be OK");
        client
            .insert_submission(&submission, &Default::default())
            .await
            .expect("Should be OK");

        let count = submission::db::count_submissions(&mut conn)
            .await
            .expect("Should be OK");
        assert_eq!(count, 4);
    }

    #[sqlx::test]
    async fn test_get_submission(pool: sqlx::SqlitePool) {
        let url = "0.0.0.0:4001";
        start_server_in_background(&pool, url).await;
        let client = Client::new(url);

        let submission = InsertSubmission {
            chunk_contents: ChunkContents::Direct {
                contents: vec![None, None, None],
            },
            metadata: None,
            strategic_metadata: Default::default(),
            chunk_size: None,
        };
        let submission_id = client
            .insert_submission(&submission, &Default::default())
            .await
            .expect("Should be OK");

        let status: SubmissionStatus = client
            .get_submission(submission_id)
            .await
            .expect("Should be OK")
            .expect("Should be Some");
        match status {
            SubmissionStatus::Completed(_) | SubmissionStatus::Failed(_, _) => {
                panic!("Expected a SubmissionStatus that is still Inprogress, got: {status:?}");
            }
            SubmissionStatus::InProgress(submission) => {
                assert_eq!(submission.chunks_done, 0);
                assert_eq!(submission.chunks_total, 3);
                assert_eq!(submission.id, submission_id);
            }
        }
    }
}

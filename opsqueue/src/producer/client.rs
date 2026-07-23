use std::time::Duration;

use backon::BackoffBuilder;
use backon::FibonacciBuilder;
use backon::Retryable;
use http::StatusCode;

use crate::{
    E,
    common::{
        StrategicMetadataMap,
        errors::E::{L, R},
        errors::{SubmissionNotCancellable, SubmissionNotFound, TooManyMatchingSubmissions},
        submission::{SubmissionId, SubmissionStatus},
    },
    tracing::CarrierMap,
};

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
    ///
    /// # Panics
    ///
    /// Panics if `host` includes a URL scheme other than `http` or `https`.
    #[must_use]
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
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
    /// Get the total number of non-failed, non-completed submissions currently
    /// known to the server.
    ///
    /// This uses the `/producer/submissions/count` endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails, the server returns a non-success
    /// status, or if the response body cannot be decoded.
    pub async fn count_submissions(&self) -> Result<u32, InternalProducerClientError> {
        (|| async {
            let base_url = &self.base_url;
            let resp = self
                .http_client
                .get(format!("{base_url}/submissions/count"))
                .send()
                .await?
                .error_for_status()?;
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
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the server returns a non-success status,
    /// or the submission id in the response cannot be decoded.
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
                .await?
                .error_for_status()?;
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

    /// Send a HTTP request to the `OpsQueue` server to cancel a submission.
    ///
    /// Will return an error if the submission is already complete, failed, or
    /// cancelled, or if the submission could not be found.
    ///
    /// # Errors
    ///
    /// Returns an error if the submission was not found, was not cancellable,
    /// or if the underlying HTTP/serialization layer fails.
    pub async fn cancel_submission(
        &self,
        submission_id: SubmissionId,
    ) -> Result<
        (),
        E![
            SubmissionNotFound,
            SubmissionNotCancellable,
            InternalProducerClientError
        ],
    > {
        (|| async {
            let base_url = &self.base_url;
            let response = self
                .http_client
                .post(format!("{base_url}/submissions/cancel/{submission_id}"))
                .send()
                .await
                .map_err(|e| R(R(e.into())))?;
            let status = response.status();
            match status {
                // 200, the submission was successfully cancelled.
                StatusCode::OK => Ok(()),
                // 404, the submission could not be found.
                StatusCode::NOT_FOUND => {
                    let not_found_err = response
                        .json::<SubmissionNotFound>()
                        .await
                        .map_err(|e| R(R(e.into())))?;
                    Err(L(not_found_err))
                }
                // 409, the submission could not be cancelled.
                StatusCode::CONFLICT => {
                    let not_cancellable_err = response
                        .json::<SubmissionNotCancellable>()
                        .await
                        .map_err(|e| R(R(e.into())))?;
                    Err(R(L(not_cancellable_err)))
                }
                _ => Err(R(R(InternalProducerClientError::UnexpectedStatus(status)))),
            }
        })
        .retry(retry_policy())
        .when(|e| match e {
            L(_) | R(L(_)) => false,
            R(R(client_err)) => client_err.is_ephemeral(),
        })
        .notify(|err, dur| {
            tracing::debug!("retrying error {err:?} with sleeping {dur:?}");
        })
        .await
    }

    /// Unpause a paused submission, making it available to consumers again.
    ///
    /// Returns an error if the submission is not currently paused.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails or the server returns an unexpected status.
    pub async fn unpause_submission(
        &self,
        submission_id: SubmissionId,
    ) -> Result<(), E![SubmissionNotFound, InternalProducerClientError]> {
        (|| async {
            let base_url = &self.base_url;
            let response = self
                .http_client
                .post(format!("{base_url}/submissions/unpause/{submission_id}"))
                .send()
                .await
                .map_err(|e| R(e.into()))?;
            let status = response.status();
            match status {
                StatusCode::OK => Ok(()),
                StatusCode::NOT_FOUND => {
                    let not_found_err = response
                        .json::<SubmissionNotFound>()
                        .await
                        .map_err(|e| R(e.into()))?;
                    Err(L(not_found_err))
                }
                _ => Err(R(InternalProducerClientError::UnexpectedStatus(status))),
            }
        })
        .retry(retry_policy())
        .when(|e| match e {
            L(_) => false,
            R(client_err) => client_err.is_ephemeral(),
        })
        .notify(|err, dur| {
            tracing::debug!("retrying error {err:?} with sleeping {dur:?}");
        })
        .await
    }

    /// Get the status of an existing submission identified by its `submission_id`.
    ///
    /// This uses the GET `/producer/submissions` endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the server responds with a non-success
    /// status, or the response payload cannot be decoded.
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
                .await?
                .error_for_status()?;
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

    /// Look up a submission id by prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the server responds with a non-success
    /// status, or the response payload cannot be decoded.
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
                .await?
                .error_for_status()?;
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

    /// Look up submission ids by strategic metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if too many submissions match, if the request fails,
    /// if the server responds with an unexpected status, or if decoding fails.
    pub async fn lookup_submission_ids_by_strategic_metadata(
        &self,
        strategic_metadata: &StrategicMetadataMap,
    ) -> Result<Vec<SubmissionId>, E![TooManyMatchingSubmissions, InternalProducerClientError]>
    {
        (|| async {
            let base_url = &self.base_url;
            let response = self
                .http_client
                .post(format!(
                    "{base_url}/submissions/lookup_ids_by_strategic_metadata"
                ))
                .json(strategic_metadata)
                .send()
                .await
                .map_err(|e| R(e.into()))?;
            let status = response.status();
            match status {
                // 200, the lookup succeeded.
                StatusCode::OK => {
                    let submission_ids = response
                        .json::<Vec<SubmissionId>>()
                        .await
                        .map_err(|e| R(e.into()))?;
                    Ok(submission_ids)
                }
                // 400, matched more submissions than the configured maximum.
                StatusCode::BAD_REQUEST => {
                    let too_many_err = response
                        .json::<TooManyMatchingSubmissions>()
                        .await
                        .map_err(|e| R(e.into()))?;
                    Err(L(too_many_err))
                }
                _ => Err(R(InternalProducerClientError::UnexpectedStatus(status))),
            }
        })
        .retry(retry_policy())
        .when(|e| match e {
            L(_) => false,
            R(client_err) => client_err.is_ephemeral(),
        })
        .notify(|err, dur| {
            tracing::debug!("retrying error {err:?} with sleeping {dur:?}");
        })
        .await
    }

    /// Get the server's version from the `/version` endpoint.
    ///
    /// A successful result will be the value of [`VERSION_CARGO_SEMVER`][crate::VERSION_CARGO_SEMVER]
    /// prefixed with a "v", for example `v0.30.5`.
    ///
    /// Upon connection failure, this will not attempt to do any retrying.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or if the server returns a non-success status.
    pub async fn server_version(&self) -> Result<String, InternalProducerClientError> {
        let base_url = &self.base_url;
        let resp = self
            .http_client
            .get(format!("{base_url}/version"))
            .send()
            .await?
            .error_for_status()?;
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
    #[error("Internal client received unexpected status: {0}")]
    UnexpectedStatus(StatusCode),
}

impl InternalProducerClientError {
    #[must_use]
    pub fn is_ephemeral(&self) -> bool {
        match self {
            // In the case of an unexpected HTTP status error, developer
            // intervention will be required.
            Self::UnexpectedStatus(_) | Self::ResponseDecodingError(_) => false,
            // In the case of an ungraceful restart, this case might theoretically trigger.
            // So even cleaner would be a tiny retry loop for this special case.
            // However, we certainly **do not** want to wait multiple minutes before returning.
            // Reqwest doesn't make this very easy as it has a single error typed used for _everything_.
            Self::HTTPClientError(inner) => {
                // Failures in which a connection could not be established are ephemeral,
                // as these can be caused by network failures, so we want to retry:
                if inner.is_connect() || inner.is_timeout() || inner.is_decode() {
                    true
                } else if inner.is_status() {
                    // Failures where the server returns a 5xx status code might indicate the server is (temporarily!) unhealthy,
                    // or in the process of a restart.
                    // Any other status is considered a permanent failure however
                    inner
                        .status()
                        .is_some_and(|status| status.is_server_error())
                } else {
                    // Anything else is a permanent failure.
                    false
                }
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
mod tests {

    use crate::{
        common::{
            StrategicMetadataMap,
            chunk::ChunkSize,
            submission::{self, SubmissionStatus},
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

    #[sqlx::test(migrator = "crate::MIGRATOR")]
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
            false,
            &mut conn,
        )
        .await
        .expect("Insertion failed");

        let count = client.count_submissions().await.expect("Should be OK");
        assert_eq!(count, 1);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
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
            strategic_metadata: StrategicMetadataMap::default(),
            chunk_size: None,
            paused: false,
        };
        client
            .insert_submission(&submission, &std::collections::HashMap::default())
            .await
            .expect("Should be OK");

        let count = submission::db::count_submissions(&mut conn)
            .await
            .expect("Should be OK");
        assert_eq!(count, 1);

        client
            .insert_submission(&submission, &std::collections::HashMap::default())
            .await
            .expect("Should be OK");
        client
            .insert_submission(&submission, &std::collections::HashMap::default())
            .await
            .expect("Should be OK");
        client
            .insert_submission(&submission, &std::collections::HashMap::default())
            .await
            .expect("Should be OK");

        let count = submission::db::count_submissions(&mut conn)
            .await
            .expect("Should be OK");
        assert_eq!(count, 4);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn test_get_submission(pool: sqlx::SqlitePool) {
        let url = "0.0.0.0:4001";
        start_server_in_background(&pool, url).await;
        let client = Client::new(url);

        let submission = InsertSubmission {
            chunk_contents: ChunkContents::Direct {
                contents: vec![None, None, None],
            },
            metadata: None,
            strategic_metadata: StrategicMetadataMap::default(),
            chunk_size: None,
            paused: false,
        };
        let submission_id = client
            .insert_submission(&submission, &std::collections::HashMap::default())
            .await
            .expect("Should be OK");

        let status: SubmissionStatus = client
            .get_submission(submission_id)
            .await
            .expect("Should be OK")
            .expect("Should be Some");
        match status {
            SubmissionStatus::Completed(_)
            | SubmissionStatus::Failed(_, _)
            | SubmissionStatus::Cancelled(_)
            | SubmissionStatus::Paused(_) => {
                panic!("Expected a SubmissionStatus that is still Inprogress, got: {status:?}");
            }
            SubmissionStatus::InProgress(submission) => {
                assert_eq!(submission.chunks_done, 0);
                assert_eq!(submission.chunks_total, 3);
                assert_eq!(submission.id, submission_id);
            }
        }
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn test_insert_paused_submission_and_unpause(pool: sqlx::SqlitePool) {
        let url = "0.0.0.0:4003";
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
            strategic_metadata: StrategicMetadataMap::default(),
            chunk_size: None,
            paused: true,
        };
        let submission_id = client
            .insert_submission(&submission, &std::collections::HashMap::default())
            .await
            .expect("Should be OK");

        let count = submission::db::count_submissions_paused(&mut conn)
            .await
            .expect("Should be OK");
        assert_eq!(count, 1);

        let status: SubmissionStatus = client
            .get_submission(submission_id)
            .await
            .expect("Should be OK")
            .expect("Should be Some");
        match status {
            SubmissionStatus::Completed(_)
            | SubmissionStatus::Failed(_, _)
            | SubmissionStatus::Cancelled(_)
            | SubmissionStatus::InProgress(_) => {
                panic!("Expected a SubmissionStatus that is Paused, got: {status:?}");
            }
            SubmissionStatus::Paused(submission) => {
                assert_eq!(submission.chunks_done, 0);
                assert_eq!(submission.chunks_total, 3);
                assert_eq!(submission.id, submission_id);
            }
        }

        client
            .unpause_submission(submission_id)
            .await
            .expect("Should be OK");

        let status: SubmissionStatus = client
            .get_submission(submission_id)
            .await
            .expect("Should be OK")
            .expect("Should be Some");
        match status {
            SubmissionStatus::Completed(_)
            | SubmissionStatus::Failed(_, _)
            | SubmissionStatus::Cancelled(_)
            | SubmissionStatus::Paused(_) => {
                panic!("Expected a SubmissionStatus that is InProgress, got: {status:?}");
            }
            SubmissionStatus::InProgress(submission) => {
                assert_eq!(submission.chunks_done, 0);
                assert_eq!(submission.chunks_total, 3);
                assert_eq!(submission.id, submission_id);
            }
        }
    }
}

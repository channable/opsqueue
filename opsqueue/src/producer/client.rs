use std::time::Duration;

use backon::BackoffBuilder;
use backon::FibonacciBuilder;
use backon::Retryable;

use crate::common::submission::{SubmissionId, SubmissionStatus};

use super::common::InsertSubmission;

fn retry_policy() -> impl BackoffBuilder {
    FibonacciBuilder::default()
    .with_jitter()
    .with_min_delay(Duration::from_millis(10))
    .with_max_delay(Duration::from_secs(5))
    .with_max_times(100)
}

#[derive(Debug, Clone)]
pub struct Client {
    pub endpoint_url: Box<str>,
    http_client: reqwest::Client,
}

impl Client {
    pub fn new(endpoint_url: &str) -> Self {
        let http_client = reqwest::Client::new();
        let endpoint_url = format!("{endpoint_url}/producer").into_boxed_str();
        Client {
            endpoint_url,
            http_client,
        }
    }

    pub async fn count_submissions(&self) -> Result<u32, InternalProducerClientError> {
        (|| async {
            let endpoint_url = &self.endpoint_url;
            let resp = self
                .http_client
                .get(format!("http://{endpoint_url}/submissions/count"))
                .send()
                .await?;
            let bytes = resp.bytes().await?;
            let body  = serde_json::from_slice(&bytes)?;

            Ok(body)
        })
        .retry(retry_policy())
        .when(InternalProducerClientError::is_ephemeral)
        .notify(|err, dur| {
            log::debug!("retrying error {:?} with sleeping {:?}", err, dur);
        })
        .await
    }

    pub async fn insert_submission(
        &self,
        submission: &InsertSubmission,
    ) -> Result<SubmissionId, InternalProducerClientError> {
        (|| async {
            let endpoint_url = &self.endpoint_url;
            let resp = self
                .http_client
                .post(format!("http://{endpoint_url}/submissions"))
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
            log::debug!("retrying error {:?} with sleeping {:?}", err, dur);
        })
        .await
    }

    pub async fn get_submission(
        &self,
        submission_id: SubmissionId,
    ) -> Result<Option<SubmissionStatus>, InternalProducerClientError> {
        (|| async {
        let endpoint_url = &self.endpoint_url;
        let resp = self
            .http_client
            .get(format!("http://{endpoint_url}/submissions/{submission_id}"))
            .send()
            .await?;
        let bytes = resp.bytes().await?;
        let body = serde_json::from_slice(&bytes)?;
        Ok(body)
        })
        .retry(retry_policy())
        .when(InternalProducerClientError::is_ephemeral)
        .notify(|err, dur| {
            log::debug!("retrying error {:?} with sleeping {:?}", err, dur);
        })
        .await
    }
}

#[derive(thiserror::Error, Debug)]
pub enum InternalProducerClientError {
    #[error("HTTP request failed")]
    HTTPClientError(#[from] reqwest::Error),
    #[error("Error decoding JSON response")]
    ResponseDecodingError(#[from] serde_json::Error)
}

impl InternalProducerClientError {
    pub fn is_ephemeral(&self) -> bool {
        match self {
            // We consider this ephemeral as on ungraceful queue shutdown/restart
            // we could have received an _incomplete_ response
            Self::ResponseDecodingError(_) => true,
            // NOTE: reqwest doesn't make this very easy as it has a single error typed used for _everything_
            // Maybe a different HTTP client library is nicer in this regard?
            Self::HTTPClientError(inner) => inner.is_connect() || inner.is_timeout() || inner.is_decode(),
        }
    }
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
mod tests {
    use ux_serde::u63;

    use crate::{
        common::submission::{self, SubmissionStatus},
        producer::common::ChunkContents,
    };

    use super::*;

    async fn start_server_in_background(pool: &sqlx::SqlitePool, url: &str) {
        tokio::spawn(
            super::super::server::serve_for_tests(pool.clone(), url.into()),
        );
        // TODO: Nicer would be a HTTP client retry loop here. Or maybe Axum has a builtin 'server has started' thing for this?
        tokio::task::yield_now().await; // Make sure that server task has a chance to run before continuing on the single-threaded tokio test runtime
    }

    #[sqlx::test]
    async fn test_count_submissions(pool: sqlx::SqlitePool) {
        // TODO: Instead of hard-coded ports, it would be nice if the server could run on a Unix domain socket when testing
        let url = "0.0.0.0:4002";
        start_server_in_background(&pool, url).await;
        let client = Client::new(url);

        let count = client.count_submissions().await.expect("Should be OK");
        assert_eq!(count, 0);

        let mut conn = pool.acquire().await.unwrap();
        submission::db::insert_submission_from_chunks(None, vec![None, None, None], None, &mut conn)
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

        let count = submission::db::count_submissions(&pool)
            .await
            .expect("Should be OK");
        assert_eq!(count, 0);

        let submission = InsertSubmission {
            chunk_contents: ChunkContents::Direct {
                contents: vec![None, None, None],
            },
            metadata: None,
        };
        client
            .insert_submission(&submission)
            .await
            .expect("Should be OK");

        let count = submission::db::count_submissions(&pool)
            .await
            .expect("Should be OK");
        assert_eq!(count, 1);

        client
            .insert_submission(&submission)
            .await
            .expect("Should be OK");
        client
            .insert_submission(&submission)
            .await
            .expect("Should be OK");
        client
            .insert_submission(&submission)
            .await
            .expect("Should be OK");

        let count = submission::db::count_submissions(&pool)
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
        };
        let submission_id = client
            .insert_submission(&submission)
            .await
            .expect("Should be OK");

        let status: SubmissionStatus = client
            .get_submission(submission_id)
            .await
            .expect("Should be OK")
            .expect("Should be Some");
        match status {
            SubmissionStatus::Completed(_) | SubmissionStatus::Failed(_) => {
                panic!(
                    "Expected a SubmissionStatus that is still Inprogress, got: {:?}",
                    status
                );
            }
            SubmissionStatus::InProgress(submission) => {
                assert_eq!(submission.chunks_done, u63::new(0).into());
                assert_eq!(submission.chunks_total, u63::new(3).into());
                assert_eq!(submission.id, submission_id);
            }
        }
    }
}

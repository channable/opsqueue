use crate::common::submission::{SubmissionId, SubmissionStatus};

use super::common::{InsertSubmission, InsertSubmissionResponse};

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
        let endpoint_url = &self.endpoint_url;
        let resp = self
            .http_client
            .get(format!("http://{endpoint_url}/submissions/count"))
            .send()
            .await?;
        let body: u32 = resp.json().await?;
        Ok(body)
    }

    pub async fn insert_submission(
        &self,
        submission: &InsertSubmission,
    ) -> Result<SubmissionId, InternalProducerClientError> {
        let endpoint_url = &self.endpoint_url;
        let resp = self
            .http_client
            .post(format!("http://{endpoint_url}/submissions"))
            .json(submission)
            .send()
            .await?;
        let body: InsertSubmissionResponse = resp.json().await?;
        Ok(body.id)
    }

    pub async fn get_submission(
        &self,
        submission_id: SubmissionId,
    ) -> Result<Option<SubmissionStatus>, InternalProducerClientError> {
        let endpoint_url = &self.endpoint_url;
        let resp = self
            .http_client
            .get(format!("http://{endpoint_url}/submissions/{submission_id}"))
            .send()
            .await?;
        let body: Option<SubmissionStatus> = resp.json().await?;
        Ok(body)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum InternalProducerClientError {
    #[error("HTTP request failed")]
    HTTPClientError(#[from] reqwest::Error),
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
mod tests {
    use crate::{
        common::submission::{self, SubmissionStatus},
        producer::common::ChunkContents,
    };

    use super::*;

    async fn start_server_in_background(pool: &sqlx::SqlitePool, url: &str) {
        tokio::spawn(
            super::super::server::ServerState::new(pool.clone()).serve_for_tests(url.into()),
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
        submission::insert_submission_from_chunks(None, vec![None, None, None], None, &mut conn)
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

        let count = submission::count_submissions(&pool)
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

        let count = submission::count_submissions(&pool)
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

        let count = submission::count_submissions(&pool)
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
                assert_eq!(submission.chunks_done, 0);
                assert_eq!(submission.chunks_total, 3);
                assert_eq!(submission.id, submission_id);
            }
        }
    }
}

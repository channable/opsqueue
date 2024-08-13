use crate::common::submission::SubmissionStatus;

use super::server::{InsertSubmission, InsertSubmissionResponse};


pub struct Client {
  endpoint_url: Box<str>,
  http_client: reqwest::Client,
}

impl Client {
    pub fn new(endpoint_url: Box<str>) -> Self {
        let http_client = reqwest::Client::new();
        Client{endpoint_url, http_client}
    }

    pub async fn count_submissionns(&self) -> Result<u32, reqwest::Error> {
        let endpoint_url = &self.endpoint_url;
        let resp = self.http_client.get(format!("http://{endpoint_url}/submissions/count")).send().await?;
        let body: u32 = resp.json().await?;
        Ok(body)
    }

    pub async fn insert_submission(&self, submission: &InsertSubmission) -> Result<i64, reqwest::Error> {
        let endpoint_url = &self.endpoint_url;
        let resp = self.http_client.post(format!("http://{endpoint_url}/submissions")).json(submission).send().await?;
        dbg!(&resp);
        let body: InsertSubmissionResponse = resp.json().await?;
        Ok(body.id)
    }

    pub async fn get_submission(&self, submission_id: i64) -> Result<SubmissionStatus, reqwest::Error> {
        let endpoint_url = &self.endpoint_url;
        let resp = self.http_client.get(format!("http://{endpoint_url}/submissions/{submission_id}")).send().await?;
        dbg!(&resp);
        let body: SubmissionStatus = resp.json().await?;
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use crate::common::submission::{self, Submission, SubmissionStatus};

    use super::*;

    async fn start_server_in_background(pool: &sqlx::SqlitePool, url: &str) {
        tokio::spawn(super::super::server::ServerState::new_from_pool(pool.clone()).serve(url.into()));
        // TODO: Nicer would be a HTTP client retry loop here. Or maybe Axum has a builtin 'server has started' thing for this?
        tokio::task::yield_now().await; // Make sure that server task has a chance to run before continuing on the single-threaded tokio test runtime
    }

    #[sqlx::test]
    async fn test_count_submissions(pool: sqlx::SqlitePool) {
        // TODO: Instead of hard-coded ports, it would be nice if the server could run on a Unix domain socket when testing
        let url: Box<str> = Box::from("0.0.0.0:3999");
        start_server_in_background(&pool, &url).await;
        let client = Client::new(url);

        let count = client.count_submissionns().await.expect("Should be OK");
        assert_eq!(count, 0);

        let mut conn = pool.acquire().await.unwrap();
        submission::insert_submission_from_chunks(None, vec!["foo".into(), "bar".into(), "baz".into()], &mut conn).await.expect("Insertion failed");

        let count = client.count_submissionns().await.expect("Should be OK");
        assert_eq!(count, 1);
    }

    #[sqlx::test]
    async fn test_insert_submission(pool: sqlx::SqlitePool) {
        let url: Box<str> = Box::from("0.0.0.0:4000");
        start_server_in_background(&pool, &url).await;
        let client = Client::new(url);

        let count = submission::count_submissions(&pool).await.expect("Should be OK");
        assert_eq!(count, 0);

        let submission = InsertSubmission { directory_uri: "test_directory".into(), chunk_count: 3, metadata: None};
        client.insert_submission(&submission).await.expect("Should be OK");

        let count = submission::count_submissions(&pool).await.expect("Should be OK");
        assert_eq!(count, 1);

        client.insert_submission(&submission).await.expect("Should be OK");
        client.insert_submission(&submission).await.expect("Should be OK");
        client.insert_submission(&submission).await.expect("Should be OK");


        let count = submission::count_submissions(&pool).await.expect("Should be OK");
        assert_eq!(count, 4);
    }

    #[sqlx::test]
    async fn test_get_submission(pool: sqlx::SqlitePool) {
        let url: Box<str> = Box::from("0.0.0.0:4001");
        start_server_in_background(&pool, &url).await;
        let client = Client::new(url);

        let submission = InsertSubmission { directory_uri: "test_directory".into(), chunk_count: 3, metadata: None};
        let submission_id = client.insert_submission(&submission).await.expect("Should be OK");

        let status: SubmissionStatus = client.get_submission(submission_id).await.expect("Should be OK");
        match status {
            SubmissionStatus::Completed(_) | SubmissionStatus::Failed(_) => {
                panic!("Expected a SubmissionStatus that is still Inprogress, got: {:?}", status);
            },
            SubmissionStatus::InProgress(submission) => {
                assert_eq!(submission.chunks_done, 0);
                assert_eq!(submission.chunks_total, 3);
                assert_eq!(submission.id, submission_id);
            }
        }
    }
}

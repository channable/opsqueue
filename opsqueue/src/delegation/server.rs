use crate::common::submission::{Metadata, SubmissionId};
use crate::common::{chunk, submission, StrategicMetadataMap};
use crate::config::Config;
use crate::db;
use crate::db::DBPools;
use crate::producer::{ChunkContents, InsertSubmission};
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use std::sync::Arc;
use tokio::select;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct ServerState {
    pool: DBPools,
    config: &'static Config,
}

impl ServerState {
    pub fn new(pool: DBPools, config: &'static Config) -> Self {
        Self { pool, config }
    }

    pub fn build_router(self: ServerState) -> Router<()> {
        Router::new()
            .route("/job/delegate", post(job_delegate))
            .route("/job/kill", post(job_kill))
            .route("/job/return", post(job_return))
            .with_state(self)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub String);

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct JobDelegationPayload {
    pub submission_id: SubmissionId,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct JobDelegation {
    // NOTE: We ignore most fields, since we do not care about them.
    pub payload: JobDelegationPayload,
    pub task_id: TaskId,
}

// TODO(delegation): Add a `jm_task_id: Option<String>` column to the `submission` and
//  `submissions_{completed,failed,canceled}` table. Change the clean-up worker s.t. it never
//  deletes a row
//  that has `jm_task_id` set. These should be handled in the `run_in_background` function in this
//  module instead.

#[tracing::instrument(level = "debug", skip(state))]
async fn job_delegate(
    State(state): State<ServerState>,
    Json(request): Json<JobDelegation>,
) -> Result<(), ()> {
    // TODO(delegation): implement this.
    todo!("check that the submission exists and set the `jm_task_id` column if it is not populated yet.");
}

#[tracing::instrument(level = "debug", skip(state))]
async fn job_kill(
    State(state): State<ServerState>,
    Json(request): Json<Vec<TaskId>>,
) -> Result<(), ()> {
    todo!()
}

#[tracing::instrument(level = "debug", skip(state))]
async fn job_return(
    State(state): State<ServerState>,
    Json(request): Json<Vec<TaskId>>,
) -> Result<(), ()> {
    todo!()
}

const JOBMACHINE_DELEGATION_BACKGROUND_LOOP: std::time::Duration =
    std::time::Duration::from_secs(5);

async fn run_in_background(notify_on_submission_change: Arc<Notify>, cancellation_token: CancellationToken) -> Result<(), ()> {
    // TODO(delegation): Implement this.
    loop {
        let triggered_by_timeout: bool = select! {
            _ = cancellation_token.cancelled() => break,
            _ = notify_on_submission_change.notified() => false,
            _ = tokio::time::sleep(JOBMACHINE_DELEGATION_BACKGROUND_LOOP) => true,
        };
        // Look for submissions that were completed/failed/cancelled that haven't been reported to JM yet.
        // That is, rows in `submissions_{completed,failed,canceled}` where `jm_task_id IS NOT NULL`.

        // Make the necessary calls to Jobmachine.

        // If `triggered_by_timeout`, log a warning that there might be a missing call to `notify_on_submission_change`.
    };

    Ok(())
}

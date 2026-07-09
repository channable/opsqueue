use crate::common::submission::SubmissionId;
use crate::config::Config;
use crate::db::{Connection as _, DBPools};
use axum::extract::State;
use axum::http::StatusCode;
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
    /// Notified whenever a submission transitions to a terminal state (completed/failed/cancelled),
    /// so the background loop can promptly report it to the JM master.
    pub notify_on_submission_change: Arc<Notify>,
    http_client: reqwest::Client,
}

impl ServerState {
    pub fn new(pool: DBPools, config: &'static Config) -> Self {
        Self {
            pool,
            config,
            notify_on_submission_change: Arc::new(Notify::new()),
            http_client: reqwest::Client::new(),
        }
    }

    pub fn run_background(self, cancellation_token: CancellationToken) -> Self {
        let state = self.clone();
        tokio::spawn(async move {
            run_in_background(
                state.notify_on_submission_change.clone(),
                state,
                cancellation_token,
            )
            .await
            .ok();
        });
        self
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

#[derive(serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum JobCompletionStatus {
    Success,
    Failure { failure_reason: &'static str },
}

#[derive(serde::Serialize)]
struct DelegatedJobCompletion {
    task_id: String,
    completion: JobCompletionStatus,
}

#[tracing::instrument(level = "debug", skip(state))]
async fn job_delegate(
    State(state): State<ServerState>,
    Json(request): Json<JobDelegation>,
) -> Result<(), StatusCode> {
    let mut conn = state.pool.writer_conn().await.map_err(|e| {
        tracing::error!("DB error acquiring writer connection: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let submission_id = request.payload.submission_id;
    let task_id = &request.task_id.0;

    // Attempt to set jm_task_id on the submission, in whichever table it currently lives.
    // The AND jm_task_id IS NULL guard makes this idempotent: if the master retries the
    // same request after an already-successful response, we simply skip the update.
    let n1 = sqlx::query!(
        "UPDATE submissions SET jm_task_id = $1 WHERE id = $2 AND jm_task_id IS NULL",
        task_id,
        submission_id,
    )
    .execute(conn.get_inner())
    .await
    .map_err(|e| {
        tracing::error!("DB error setting jm_task_id on submissions: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .rows_affected();

    let n2 = sqlx::query!(
        "UPDATE submissions_completed SET jm_task_id = $1 WHERE id = $2 AND jm_task_id IS NULL",
        task_id,
        submission_id,
    )
    .execute(conn.get_inner())
    .await
    .map_err(|e| {
        tracing::error!("DB error setting jm_task_id on submissions_completed: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .rows_affected();

    let n3 = sqlx::query!(
        "UPDATE submissions_failed SET jm_task_id = $1 WHERE id = $2 AND jm_task_id IS NULL",
        task_id,
        submission_id,
    )
    .execute(conn.get_inner())
    .await
    .map_err(|e| {
        tracing::error!("DB error setting jm_task_id on submissions_failed: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .rows_affected();

    let n4 = sqlx::query!(
        "UPDATE submissions_cancelled SET jm_task_id = $1 WHERE id = $2 AND jm_task_id IS NULL",
        task_id,
        submission_id,
    )
    .execute(conn.get_inner())
    .await
    .map_err(|e| {
        tracing::error!("DB error setting jm_task_id on submissions_cancelled: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .rows_affected();

    let updated = n1 + n2 + n3 + n4;

    if updated > 0 {
        // Trigger the background loop in case the submission already reached a terminal state.
        state.notify_on_submission_change.notify_one();
        return Ok(());
    }

    // No rows were updated. Either jm_task_id was already set (retry) or the submission
    // doesn't exist. Check existence across all tables to distinguish the two cases.
    let count = sqlx::query!(
        r#"SELECT COUNT(1) as "count: i64" FROM (
            SELECT id FROM submissions WHERE id = $1
            UNION ALL
            SELECT id FROM submissions_completed WHERE id = $2
            UNION ALL
            SELECT id FROM submissions_failed WHERE id = $3
            UNION ALL
            SELECT id FROM submissions_cancelled WHERE id = $4
        )"#,
        submission_id,
        submission_id,
        submission_id,
        submission_id,
    )
    .fetch_one(conn.get_inner())
    .await
    .map_err(|e| {
        tracing::error!("DB error checking submission existence: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .count;

    if count > 0 {
        // Submission exists; jm_task_id was already set on a previous request. Idempotent OK.
        Ok(())
    } else {
        tracing::warn!(%submission_id, "Received job_delegate for unknown submission");
        Err(StatusCode::NOT_FOUND)
    }
}

#[tracing::instrument(level = "debug", skip(state))]
async fn job_kill(
    State(state): State<ServerState>,
    Json(request): Json<Vec<TaskId>>,
) -> Result<(), ()> {
    let _ = (state, request);
    todo!()
}

#[tracing::instrument(level = "debug", skip(state))]
async fn job_return(
    State(state): State<ServerState>,
    Json(request): Json<Vec<TaskId>>,
) -> Result<(), ()> {
    let _ = (state, request);
    todo!()
}

const JOBMACHINE_DELEGATION_BACKGROUND_LOOP: std::time::Duration =
    std::time::Duration::from_secs(5);

async fn run_in_background(
    notify_on_submission_change: Arc<Notify>,
    state: ServerState,
    cancellation_token: CancellationToken,
) -> Result<(), ()> {
    let Some(jm_master_url) = &state.config.jm_master_url else {
        return Ok(());
    };

    match report_delegated_submissions(&state, jm_master_url, false).await {
        Ok(()) => {}
        Err(e) => tracing::error!("Error in delegation background loop: {e:?}"),
    }

    loop {
        let triggered_by_timeout: bool = select! {
            _ = cancellation_token.cancelled() => break,
            _ = notify_on_submission_change.notified() => false,
            _ = tokio::time::sleep(JOBMACHINE_DELEGATION_BACKGROUND_LOOP) => true,
        };

        match report_delegated_submissions(&state, jm_master_url, triggered_by_timeout).await {
            Ok(()) => {}
            Err(e) => tracing::error!("Error in delegation background loop: {e:?}"),
        }
    }

    Ok(())
}

/// Queries all terminal submissions with `jm_task_id IS NOT NULL`, reports them to the
/// JM master via `/delegation/complete`, then deletes the rows (along with their
/// associated chunks and metadata) on success.
async fn report_delegated_submissions(
    state: &ServerState,
    jm_master_url: &url::Url,
    triggered_by_timeout: bool,
) -> anyhow::Result<()> {
    let mut conn = state.pool.reader_conn().await?;

    let completed = sqlx::query!(
        r#"SELECT id AS "id: SubmissionId", jm_task_id
        FROM submissions_completed WHERE jm_task_id IS NOT NULL"#
    )
    .fetch_all(conn.get_inner())
    .await?;

    let failed = sqlx::query!(
        r#"SELECT id AS "id: SubmissionId", jm_task_id
        FROM submissions_failed WHERE jm_task_id IS NOT NULL"#
    )
    .fetch_all(conn.get_inner())
    .await?;

    let cancelled = sqlx::query!(
        r#"SELECT id AS "id: SubmissionId", jm_task_id
        FROM submissions_cancelled WHERE jm_task_id IS NOT NULL"#
    )
    .fetch_all(conn.get_inner())
    .await?;

    drop(conn);

    let tasks_to_report: Vec<DelegatedJobCompletion> = completed
        .iter()
        .map(|r| DelegatedJobCompletion {
            task_id: r.jm_task_id.clone().unwrap(),
            completion: JobCompletionStatus::Success,
        })
        .chain(failed.iter().map(|r| DelegatedJobCompletion {
            task_id: r.jm_task_id.clone().unwrap(),
            completion: JobCompletionStatus::Failure {
                // TODO(delegation): Distinguish between "known" and "unknown".
                failure_reason: "unknown",
            },
        }))
        .chain(cancelled.iter().map(|r| DelegatedJobCompletion {
            task_id: r.jm_task_id.clone().unwrap(),
            completion: JobCompletionStatus::Failure {
                // TODO(delegation): What should the failure reason for cancellations be?
                failure_reason: "unknown",
            },
        }))
        .collect();

    if tasks_to_report.is_empty() {
        return Ok(());
    }

    if triggered_by_timeout {
        tracing::warn!(
            n_tasks = tasks_to_report.len(),
            "Delegation background loop triggered by timeout with pending tasks; \
             possible missing notify_on_submission_change call"
        );
    }

    let url = format!(
        "{}/delegation/complete",
        jm_master_url.as_str().trim_end_matches('/')
    );
    state
        .http_client
        .put(&url)
        .json(&tasks_to_report)
        .send()
        .await?
        .error_for_status()?;

    // Report succeeded; clear jm_task_id on all reported rows so the periodic cleanup
    // worker can delete them in due course.
    let mut conn = state.pool.writer_conn().await?;
    clear_jm_task_id("submissions_completed", &completed.iter().map(|r| r.id).collect::<Vec<_>>(), &mut conn).await?;
    clear_jm_task_id("submissions_failed", &failed.iter().map(|r| r.id).collect::<Vec<_>>(), &mut conn).await?;
    clear_jm_task_id("submissions_cancelled", &cancelled.iter().map(|r| r.id).collect::<Vec<_>>(), &mut conn).await?;

    Ok(())
}

/// Sets `jm_task_id = NULL` for all given submission IDs in the specified table,
/// making those rows eligible for deletion by the periodic cleanup worker.
async fn clear_jm_task_id(
    table: &str,
    ids: &[SubmissionId],
    conn: &mut impl crate::db::WriterConnection,
) -> sqlx::Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let mut qb = sqlx::QueryBuilder::new("UPDATE ");
    qb.push(table);
    qb.push(" SET jm_task_id = NULL WHERE id IN (");
    let mut sep = qb.separated(", ");
    for id in ids {
        sep.push_bind(*id);
    }
    qb.push(")");
    qb.build().execute(conn.get_inner()).await?;
    Ok(())
}

use crate::common::errors::{E, SubmissionNotFound};
use crate::common::submission::{self, SubmissionId};
use crate::config::Config;
use crate::db::{Connection, DBPools, WriterConnection};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use std::sync::Arc;
use tokio::select;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
pub(crate) fn app_for_tests(
    pool: DBPools,
    cancellation_token: &CancellationToken,
    delegation_server_url: url::Url,
    notify_on_submission_change: Arc<Notify>,
) -> Router {
    let notify_on_insert = Arc::new(Notify::new());
    let config: &mut Config = Box::leak(Box::default());
    config.delegation_server_url = Some(delegation_server_url);
    let router = ServerState::new(
        pool,
        config,
        cancellation_token.clone(),
        notify_on_insert,
        notify_on_submission_change,
    )
    .run_background()
    .build_router();

    Router::new().nest("/job", router)
}

#[derive(Debug, Clone)]
pub struct ServerState {
    pool: DBPools,
    cancellation_token: CancellationToken,
    /// Notified when new chunks become available for dispatch (e.g. after unpausing a submission).
    pub notify_on_insert: Arc<Notify>,
    /// Notified whenever a submission changes status, so the background loop can report
    /// it to the external service.
    pub notify_on_submission_change: Arc<Notify>,
    delegation_server_url: url::Url,
    http_client: reqwest::Client,
}

impl ServerState {
    /// # Panics
    ///
    /// Panics if `config.delegation_server_url` is not set.
    pub fn new(
        pool: DBPools,
        config: &'static Config,
        cancellation_token: CancellationToken,
        notify_on_insert: Arc<Notify>,
        notify_on_submission_change: Arc<Notify>,
    ) -> Self {
        Self {
            pool,
            cancellation_token,
            notify_on_insert,
            notify_on_submission_change,
            delegation_server_url: config
                .delegation_server_url
                .clone()
                .expect("delegation_server_url not set"),
            http_client: reqwest::Client::new(),
        }
    }

    #[must_use]
    pub fn run_background(self) -> Self {
        let state = self.clone();
        let cancellation_token = self.cancellation_token.clone();
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
            .route("/delegate", post(job_delegate))
            .route("/kill", post(job_kill))
            .route("/return", post(job_return))
            // .route("/submit", post(submit))
            .with_state(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "TEXT", rename_all = "snake_case")]
enum DelegatedJobStatus {
    Paused,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

// #[derive(Debug, serde::Deserialize)]
// #[serde(tag = "type", content = "contents")]
// enum WorkerDelegationEvent {
//     #[serde(rename = "delegate")]
//     Delegate(Vec<DelegatedJob>),
//     #[serde(rename = "kill")]
//     Kill(Vec<String>),
//     #[serde(rename = "return")]
//     Return(Vec<String>),
// }
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct DelegatedJob {
    task_id: String,
    payload: DelegatedJobPayload,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct DelegatedJobPayload {
    submission_id: SubmissionId,
}

// #[derive(Debug, serde::Serialize)]
// #[serde(tag = "type", content = "contents")]
// enum MasterDelegationEvent<'a> {
//     #[serde(rename = "updated")]
//     Updated(Vec<DelegatedJobUpdate<'a>>),
//     #[serde(rename = "completed")]
//     Completed(Vec<DelegatedJobCompletion<'a>>),
// }

#[derive(Debug, serde::Serialize)]
struct DelegatedJobUpdate<'a> {
    task_id: &'a str,
    status: DelegatedJobUpdateStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum DelegatedJobUpdateStatus {
    Queued,
    Running,
}

#[derive(Debug, serde::Serialize)]
struct DelegatedJobCompletion<'a> {
    task_id: &'a str,
    completion: DelegatedJobCompletionStatus,
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "status")]
enum DelegatedJobCompletionStatus {
    #[serde(rename = "success")]
    Success,
    #[serde(rename = "failure")]
    Failure { failure_reason: FailureReason },
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum FailureReason {
    Unknown,
    Forced,
}

// TODO(delegation): Switch to
// #[tracing::instrument(level = "debug", skip(state))]
// async fn submit(
//     State(state): State<ServerState>,
//     Json(events): Json<Vec<WorkerDelegationEvent>>,
// ) -> Result<StatusCode, StatusCode> {
//     let mut conn = state.pool.writer_conn().await.map_err(|e| {
//         tracing::error!("DB error acquiring writer connection: {e:?}");
//         StatusCode::INTERNAL_SERVER_ERROR
//     })?;
//     // TODO(delegation): Operate within a transaction.
//     for event in events {
//         match event {
//             WorkerDelegationEvent::Delegate(delegations) => {
//                 for delegation in delegations {
//                     handle_delegate_event(&state, &mut conn, &delegation)
//                         .await
//                         .map_err(|e| {
//                             tracing::error!("Error handling delegate event: {e:?}");
//                             e
//                         })?;
//                 }
//             }
//             WorkerDelegationEvent::Kill(task_ids) => {
//                 for task_id in task_ids {
//                     handle_kill_event(&state, &mut conn, &task_id)
//                         .await
//                         .map_err(|e| {
//                             tracing::error!(
//                                 "Error handling kill event for task_id={task_id}: {e:?}"
//                             );
//                             e
//                         })?;
//                 }
//             }
//             WorkerDelegationEvent::Return(_task_ids) => {
//                 tracing::info!(
//                     "Received 'return' delegation event, which is not yet implemented; ignoring."
//                 );
//                 return Ok(StatusCode::ACCEPTED);
//             }
//         }
//     }
//
//     Ok(StatusCode::ACCEPTED)
// }

#[tracing::instrument(level = "debug", skip(state))]
async fn job_delegate(
    State(state): State<ServerState>,
    Json(job): Json<DelegatedJob>,
) -> Result<StatusCode, StatusCode> {
    let mut conn = state.pool.writer_conn().await.map_err(|e| {
        tracing::error!("DB error acquiring writer connection: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    handle_delegate_event(&mut conn, &job).await.map_err(|e| {
        tracing::error!("DB error handling delegate event: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state.notify_on_submission_change.notify_one();
    state.notify_on_insert.notify_waiters();

    Ok(StatusCode::ACCEPTED)
}

#[tracing::instrument(level = "debug", skip(state))]
async fn job_kill(
    State(state): State<ServerState>,
    Json(task_ids): Json<Vec<String>>,
) -> Result<StatusCode, StatusCode> {
    let mut conn = state.pool.writer_conn().await.map_err(|e| {
        tracing::error!("DB error acquiring writer connection: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    conn.transaction(move |mut tx| {
        Box::pin(async move {
            for task_id in &task_ids {
                handle_kill_event(&mut tx, task_id).await?;
            }

            Ok::<(), sqlx::Error>(())
        })
    })
    .await
    .map_err(|e| {
        tracing::error!("DB error handling kill event: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    state.notify_on_submission_change.notify_one();

    Ok(StatusCode::ACCEPTED)
}

#[tracing::instrument(level = "debug", skip(_state))]
async fn job_return(
    State(_state): State<ServerState>,
    Json(task_ids): Json<Vec<String>>,
) -> Result<StatusCode, StatusCode> {
    tracing::info!("Received 'return' delegation event, which is not yet implemented; ignoring.");

    Ok(StatusCode::ACCEPTED)
}

#[tracing::instrument(level = "debug", skip(conn))]
async fn handle_delegate_event(
    conn: &mut impl WriterConnection,
    job: &DelegatedJob,
) -> sqlx::Result<()> {
    let task_id = &job.task_id;
    let submission_id = job.payload.submission_id;

    let rows_affected = insert_external_task(&mut *conn, submission_id, task_id).await?;

    if rows_affected == 0 {
        tracing::debug!(%submission_id, %task_id, "External task was already registered");
    }

    match submission::db::unpause_submission(submission_id, &mut *conn).await {
        Ok(()) => {}
        Err(E::R(SubmissionNotFound(_))) => {
            tracing::debug!(%submission_id, "Submission was not in paused state; assuming already active");
        }
        Err(E::L(db_err)) => {
            tracing::error!(%submission_id, "DB error unpausing submission: {db_err:?}");
            return Err(db_err.0);
        }
    }

    Ok(())
}

#[tracing::instrument(level = "debug", skip(conn))]
async fn handle_kill_event(conn: &mut impl WriterConnection, task_id: &str) -> sqlx::Result<()> {
    let submission_id = sqlx::query_scalar!(
        r#"SELECT submission_id AS "submission_id: SubmissionId"
           FROM submissions_external_task
           WHERE task_id = $1"#,
        task_id,
    )
    .fetch_optional(conn.get_inner())
    .await?;

    let Some(submission_id) = submission_id else {
        tracing::warn!(%task_id, "Kill event for unknown task_id; ignoring");
        return Ok(());
    };

    match submission::db::cancel_submission_notx(submission_id, conn).await {
        Ok(()) => {}
        Err(E::L(db_err)) => {
            tracing::error!(%submission_id, "DB error cancelling submission: {db_err:?}");
            return Err(db_err.0);
        }
        Err(E::R(SubmissionNotFound(_))) => {
            tracing::warn!(%submission_id, "Submission not found when attempting to cancel; already gone");
        }
    }

    Ok(())
}

const DELEGATION_BACKGROUND_LOOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

async fn run_in_background(
    notify_on_submission_change: Arc<Notify>,
    state: ServerState,
    cancellation_token: CancellationToken,
) -> Result<(), ()> {
    tracing::info!(
        "Started delegation background loop. Updates will be sent to {}",
        state.delegation_server_url
    );

    let mut triggered_by_timeout: bool = false;

    loop {
        match report_submission_status(&state, triggered_by_timeout).await {
            Ok(()) => {}
            Err(e) => tracing::error!("Error in delegation background loop: {e:?}"),
        }

        triggered_by_timeout = select! {
            () = cancellation_token.cancelled() => break,
            () = notify_on_submission_change.notified() => false,
            () = tokio::time::sleep(DELEGATION_BACKGROUND_LOOP_TIMEOUT) => true,
        };
    }

    Ok(())
}

async fn report_submission_status(
    state: &ServerState,
    triggered_by_timeout: bool,
) -> anyhow::Result<()> {
    let out_of_date_tasks = {
        let conn = state.pool.reader_conn().await?;
        select_out_of_date_tasks(conn).await?
    };

    if out_of_date_tasks.is_empty() {
        return Ok(());
    }

    if triggered_by_timeout {
        tracing::warn!(
            n_out_of_date_tasks = out_of_date_tasks.len(),
            "Delegation background loop triggered by timeout with pending tasks; \
             possible missing notify_on_submission_change call"
        );
    }

    for batch in out_of_date_tasks.chunks(2048) {
        let mut updates = Vec::new();
        let mut completions = Vec::new();

        for task in batch {
            match task.current_status {
                DelegatedJobStatus::Paused => updates.push(DelegatedJobUpdate {
                    task_id: &task.task_id,
                    status: DelegatedJobUpdateStatus::Queued,
                }),
                DelegatedJobStatus::InProgress => updates.push(DelegatedJobUpdate {
                    task_id: &task.task_id,
                    status: DelegatedJobUpdateStatus::Running,
                }),
                DelegatedJobStatus::Completed => completions.push(DelegatedJobCompletion {
                    task_id: &task.task_id,
                    completion: DelegatedJobCompletionStatus::Success,
                }),
                DelegatedJobStatus::Failed => completions.push(DelegatedJobCompletion {
                    task_id: &task.task_id,
                    completion: DelegatedJobCompletionStatus::Failure {
                        failure_reason: FailureReason::Unknown,
                    },
                }),
                DelegatedJobStatus::Cancelled => completions.push(DelegatedJobCompletion {
                    task_id: &task.task_id,
                    completion: DelegatedJobCompletionStatus::Failure {
                        failure_reason: FailureReason::Forced,
                    },
                }),
            }
        }

        if !updates.is_empty() {
            send_updates(state, &updates).await?;
            let conn = state.pool.writer_conn().await?;
            update_last_status_sent(
                conn,
                out_of_date_tasks
                    .iter()
                    .filter(|task| {
                        task.current_status == DelegatedJobStatus::Paused
                            || task.current_status == DelegatedJobStatus::InProgress
                    })
                    .collect(),
            )
            .await?;
        }

        if !completions.is_empty() {
            send_completions(state, &completions).await?;
            let conn = state.pool.writer_conn().await?;
            delete_external_tasks(
                conn,
                out_of_date_tasks
                    .iter()
                    .filter(|task| {
                        task.current_status == DelegatedJobStatus::Completed
                            || task.current_status == DelegatedJobStatus::Failed
                            || task.current_status == DelegatedJobStatus::Cancelled
                    })
                    .collect(),
            )
            .await?;
        }
    }

    Ok(())
}

async fn insert_external_task(
    mut conn: impl Connection,
    submission_id: SubmissionId,
    task_id: &str,
) -> sqlx::Result<u64> {
    let rows_affected = sqlx::query!(
        r#"INSERT INTO submissions_external_task (submission_id, task_id, last_status_sent)
           SELECT $1 AS submission_id, $2 AS task_id, NULL AS last_status_sent
           WHERE NOT EXISTS (
            SELECT TRUE
            FROM submissions_external_task
            WHERE submission_id = $1 AND task_id = $2
           )"#,
        submission_id,
        task_id,
    )
    .execute(conn.get_inner())
    .await?
    .rows_affected();

    Ok(rows_affected)
}

#[derive(Debug)]
struct OutOfDateTaskRow {
    task_id: String,
    current_status: DelegatedJobStatus,
}

async fn select_out_of_date_tasks(
    mut conn: impl Connection,
) -> sqlx::Result<Vec<OutOfDateTaskRow>> {
    sqlx::query_as!(
        OutOfDateTaskRow,
        r#"WITH out_of_date_tasks AS (
            SELECT
                submission_id,
                task_id
            FROM submissions_external_task as t
            WHERE
               t.last_status_sent IS NULL
               OR (t.last_status_sent = 'paused' AND NOT EXISTS(SELECT * FROM submissions_paused AS s WHERE s.id = t.submission_id))
               OR (t.last_status_sent = 'in_progress' AND NOT EXISTS(SELECT * FROM submissions AS s WHERE s.id = t.submission_id))
               OR (t.last_status_sent = 'completed' AND NOT EXISTS(SELECT * FROM submissions_completed AS s WHERE s.id = t.submission_id))
               OR (t.last_status_sent = 'failed' AND NOT EXISTS(SELECT * FROM submissions_failed AS s WHERE s.id = t.submission_id))
               OR (t.last_status_sent = 'cancelled' AND NOT EXISTS(SELECT * FROM submissions_cancelled AS s WHERE s.id = t.submission_id))
           )
           SELECT
               task_id,
               coalesce(
                (SELECT 'paused' FROM submissions_paused AS s WHERE s.id = t.submission_id),
                (SELECT 'in_progress' FROM submissions AS s WHERE s.id = t.submission_id),
                (SELECT 'completed' FROM submissions_completed AS s WHERE s.id = t.submission_id),
                (SELECT 'failed' FROM submissions_failed AS s WHERE s.id = t.submission_id),
                (SELECT 'cancelled' FROM submissions_cancelled AS s WHERE s.id = t.submission_id)
               ) AS "current_status!: DelegatedJobStatus"
           FROM out_of_date_tasks AS t
        "#)
        .fetch_all(conn.get_inner())
        .await
}

async fn update_last_status_sent(
    mut conn: impl WriterConnection,
    tasks: Vec<&OutOfDateTaskRow>,
) -> sqlx::Result<()> {
    let tasks = tasks
        .iter()
        .map(|t| (t.current_status, t.task_id.clone()))
        .collect::<Vec<_>>();

    conn.transaction(move |mut tx| {
        Box::pin(async move {
            for (current_status, task_id) in tasks {
                sqlx::query!(
                    "UPDATE submissions_external_task SET last_status_sent = $1 WHERE task_id = $2",
                    current_status,
                    task_id,
                )
                .execute(tx.get_inner())
                .await?;
            }

            Ok::<_, sqlx::Error>(())
        })
    })
    .await?;

    Ok(())
}

async fn delete_external_tasks(
    mut conn: impl WriterConnection,
    tasks: Vec<&OutOfDateTaskRow>,
) -> sqlx::Result<()> {
    let tasks = tasks.iter().map(|t| t.task_id.clone()).collect::<Vec<_>>();

    conn.transaction(move |mut tx| {
        Box::pin(async move {
            for task_id in tasks {
                sqlx::query!(
                    "DELETE FROM submissions_external_task WHERE task_id = $1",
                    task_id,
                )
                .execute(tx.get_inner())
                .await?;
            }

            Ok::<_, sqlx::Error>(())
        })
    })
    .await?;

    Ok(())
}

// TODO(delegation): Replace `send_updates` and `send_completions` with `send_events`,
//  after https://github.com/channable/jobmachine/pull/2210 is merged.
// async fn send_events(
//     state: &ServerState,
//     events: &MasterDelegationEvent<'_>,
// ) -> reqwest::Result<()> {
//     state
//         .http_client
//         .put(
//             state
//                 .delegation_server_url
//                 .join("/delegation/submit")
//                 .unwrap(),
//         )
//         .json(&events)
//         .send()
//         .await?
//         .error_for_status()?;
//
//     Ok(())
// }

async fn send_updates(
    state: &ServerState,
    updates: &[DelegatedJobUpdate<'_>],
) -> reqwest::Result<()> {
    state
        .http_client
        .put(
            state
                .delegation_server_url
                .join("/delegation/update")
                .unwrap(),
        )
        .json(updates)
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}

async fn send_completions(
    state: &ServerState,
    completions: &[DelegatedJobCompletion<'_>],
) -> reqwest::Result<()> {
    state
        .http_client
        .put(
            state
                .delegation_server_url
                .join("/delegation/complete")
                .unwrap(),
        )
        .json(completions)
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {
    use crate::common::StrategicMetadataMap;
    use crate::common::chunk::db::{complete_chunk, retry_or_fail_chunk};
    use crate::common::chunk::{ChunkIndex, ChunkSize};
    use crate::common::submission::db::{
        cancel_submission, count_submissions, count_submissions_cancelled,
        count_submissions_paused, insert_submission_from_chunks, unpause_submission,
    };
    use crate::db::{Connection, DBPools};
    use crate::delegation::server::{
        DelegatedJob, DelegatedJobPayload, app_for_tests, insert_external_task,
    };
    use axum::body::Body;
    use axum::http::Request;
    use http::{StatusCode, header};
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use tokio::sync::{Notify, oneshot};
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt;
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

    struct SignalResponder {
        sender: Mutex<Option<oneshot::Sender<()>>>,
        response: ResponseTemplate,
    }

    impl SignalResponder {
        fn new(sender: oneshot::Sender<()>, response: ResponseTemplate) -> Self {
            Self {
                sender: Mutex::new(Some(sender)),
                response,
            }
        }
    }

    impl Respond for SignalResponder {
        fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
            if let Ok(mut lock) = self.sender.lock()
                && let Some(tx) = lock.take()
            {
                let _ = tx.send(());
            }
            self.response.clone()
        }
    }

    async fn count_external_tasks(mut db: impl Connection) -> sqlx::Result<u64> {
        let count = sqlx::query_scalar!("SELECT COUNT(*) as count FROM submissions_external_task;")
            .fetch_one(db.get_inner())
            .await?;
        Ok(u64::try_from(count).expect("COUNT(*) is always non-negative"))
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_job_delegation(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let reader_pool = pool_opts
            .clone()
            .max_connections(16)
            .connect_with(conn_opts.clone())
            .await
            .unwrap();
        let writer_pool = pool_opts
            .max_connections(1)
            .connect_with(conn_opts)
            .await
            .unwrap();
        let pool = DBPools::from_test_pools(&reader_pool, &writer_pool);

        let external_server = MockServer::start().await;

        let cancellation_token = CancellationToken::new();
        let app = app_for_tests(
            pool.clone(),
            &cancellation_token,
            external_server.uri().parse().unwrap(),
            Arc::new(Notify::new()),
        );

        let submission = {
            let mut conn = pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                true,
                &mut conn,
            )
            .await
            .unwrap()
        };

        {
            let mut conn = pool.reader_conn().await.unwrap();
            assert_eq!(count_submissions_paused(&mut conn).await.unwrap(), 1);
            assert_eq!(count_external_tasks(&mut conn).await.unwrap(), 0);
        }

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("PUT"))
            .and(path("/delegation/update"))
            .and(body_partial_json(
                json!([{"task_id": "test", "status": "running"}]),
            ))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&external_server)
            .await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/job/delegate")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_string(&DelegatedJob {
                            task_id: "test".to_string(),
                            payload: DelegatedJobPayload {
                                submission_id: submission,
                            },
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::ACCEPTED,
            "request failed: {response:?}"
        );

        tokio::time::timeout(std::time::Duration::from_secs(2), rx)
            .await
            .expect("Timed out waiting for HTTP request")
            .expect("Sender dropped without signaling");

        {
            let mut conn = pool.reader_conn().await.unwrap();
            assert_eq!(count_external_tasks(&mut conn).await.unwrap(), 1);
            assert_eq!(count_submissions(&mut conn).await.unwrap(), 1);
        }
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_job_kill(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let reader_pool = pool_opts
            .clone()
            .max_connections(16)
            .connect_with(conn_opts.clone())
            .await
            .unwrap();
        let writer_pool = pool_opts
            .max_connections(1)
            .connect_with(conn_opts)
            .await
            .unwrap();
        let pool = DBPools::from_test_pools(&reader_pool, &writer_pool);

        let external_server = MockServer::start().await;

        let cancellation_token = CancellationToken::new();
        let app = app_for_tests(
            pool.clone(),
            &cancellation_token,
            external_server.uri().parse().unwrap(),
            Arc::new(Notify::new()),
        );

        {
            let mut conn = pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            let submission = insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                true,
                &mut conn,
            )
            .await
            .unwrap();

            insert_external_task(&mut conn, submission, "test")
                .await
                .unwrap();
        };

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("PUT"))
            .and(path("/delegation/complete"))
            .and(body_partial_json(json!([{"task_id": "test", "completion": {"status": "failure", "failure_reason": "forced"}}])))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&external_server)
            .await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/job/kill")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&["test"]).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::ACCEPTED,
            "request failed: {response:?}"
        );

        {
            let mut conn = pool.reader_conn().await.unwrap();
            assert_eq!(count_submissions_cancelled(&mut conn).await.unwrap(), 1);
            assert_eq!(count_submissions(&mut conn).await.unwrap(), 0);
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), rx)
            .await
            .expect("Timed out waiting for HTTP request")
            .expect("Sender dropped without signaling");

        // Wait for background loop to remove external tasks;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        {
            let mut conn = pool.reader_conn().await.unwrap();
            assert_eq!(count_external_tasks(&mut conn).await.unwrap(), 0);
        }
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_unpause_update(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let reader_pool = pool_opts
            .clone()
            .max_connections(16)
            .connect_with(conn_opts.clone())
            .await
            .unwrap();
        let writer_pool = pool_opts
            .max_connections(1)
            .connect_with(conn_opts)
            .await
            .unwrap();
        let pool = DBPools::from_test_pools(&reader_pool, &writer_pool);

        let external_server = MockServer::start().await;

        let cancellation_token = CancellationToken::new();
        let notify_on_submission_change = Arc::new(Notify::new());
        let _ = app_for_tests(
            pool.clone(),
            &cancellation_token,
            external_server.uri().parse().unwrap(),
            notify_on_submission_change.clone(),
        );

        let submission = {
            let mut conn = pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            let submission = insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                true,
                &mut conn,
            )
            .await
            .unwrap();

            insert_external_task(&mut conn, submission, "test")
                .await
                .unwrap();

            submission
        };

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("PUT"))
            .and(path("/delegation/update"))
            .and(body_partial_json(
                json!([{"task_id": "test", "status": "running"}]),
            ))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&external_server)
            .await;

        {
            let mut conn = pool.writer_conn().await.unwrap();
            unpause_submission(submission, &mut conn).await.unwrap();
            notify_on_submission_change.notify_one();
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), rx)
            .await
            .expect("Timed out waiting for HTTP request")
            .expect("Sender dropped without signaling");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_complete_update(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let reader_pool = pool_opts
            .clone()
            .max_connections(16)
            .connect_with(conn_opts.clone())
            .await
            .unwrap();
        let writer_pool = pool_opts
            .max_connections(1)
            .connect_with(conn_opts)
            .await
            .unwrap();
        let pool = DBPools::from_test_pools(&reader_pool, &writer_pool);

        let external_server = MockServer::start().await;

        let cancellation_token = CancellationToken::new();
        let notify_on_submission_change = Arc::new(Notify::new());
        let _ = app_for_tests(
            pool.clone(),
            &cancellation_token,
            external_server.uri().parse().unwrap(),
            notify_on_submission_change.clone(),
        );

        let submission = {
            let mut conn = pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            let submission = insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                false,
                &mut conn,
            )
            .await
            .unwrap();

            insert_external_task(&mut conn, submission, "test")
                .await
                .unwrap();

            submission
        };

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("PUT"))
            .and(path("/delegation/complete"))
            .and(body_partial_json(
                json!([{"task_id": "test", "completion": {"status": "success"}}]),
            ))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&external_server)
            .await;

        {
            let mut conn = pool.writer_conn().await.unwrap();
            complete_chunk((submission, ChunkIndex::zero()).into(), None, &mut conn)
                .await
                .unwrap();
            notify_on_submission_change.notify_one();
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), rx)
            .await
            .expect("Timed out waiting for HTTP request")
            .expect("Sender dropped without signaling");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_fail_update(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let reader_pool = pool_opts
            .clone()
            .max_connections(16)
            .connect_with(conn_opts.clone())
            .await
            .unwrap();
        let writer_pool = pool_opts
            .max_connections(1)
            .connect_with(conn_opts)
            .await
            .unwrap();
        let pool = DBPools::from_test_pools(&reader_pool, &writer_pool);

        let external_server = MockServer::start().await;

        let cancellation_token = CancellationToken::new();
        let notify_on_submission_change = Arc::new(Notify::new());
        let _ = app_for_tests(
            pool.clone(),
            &cancellation_token,
            external_server.uri().parse().unwrap(),
            notify_on_submission_change.clone(),
        );

        let submission = {
            let mut conn = pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            let submission = insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                false,
                &mut conn,
            )
            .await
            .unwrap();

            insert_external_task(&mut conn, submission, "test")
                .await
                .unwrap();

            submission
        };

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("PUT"))
            .and(path("/delegation/complete"))
            .and(body_partial_json(json!([{"task_id": "test", "completion": {"status": "failure", "failure_reason": "unknown"}}])))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&external_server)
            .await;

        {
            let mut conn = pool.writer_conn().await.unwrap();
            retry_or_fail_chunk(
                (submission, ChunkIndex::zero()).into(),
                "extreme error".to_owned(),
                &mut conn,
                0,
            )
            .await
            .unwrap();
            notify_on_submission_change.notify_one();
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), rx)
            .await
            .expect("Timed out waiting for HTTP request")
            .expect("Sender dropped without signaling");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_cancel_update(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let reader_pool = pool_opts
            .clone()
            .max_connections(16)
            .connect_with(conn_opts.clone())
            .await
            .unwrap();
        let writer_pool = pool_opts
            .max_connections(1)
            .connect_with(conn_opts)
            .await
            .unwrap();
        let pool = DBPools::from_test_pools(&reader_pool, &writer_pool);

        let external_server = MockServer::start().await;

        let cancellation_token = CancellationToken::new();
        let notify_on_submission_change = Arc::new(Notify::new());
        let _ = app_for_tests(
            pool.clone(),
            &cancellation_token,
            external_server.uri().parse().unwrap(),
            notify_on_submission_change.clone(),
        );

        let submission = {
            let mut conn = pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            let submission = insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                false,
                &mut conn,
            )
            .await
            .unwrap();

            insert_external_task(&mut conn, submission, "test")
                .await
                .unwrap();

            submission
        };

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("PUT"))
            .and(path("/delegation/complete"))
            .and(body_partial_json(json!([{"task_id": "test", "completion": {"status": "failure", "failure_reason": "forced"}}])))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&external_server)
            .await;

        {
            let mut conn = pool.writer_conn().await.unwrap();
            cancel_submission(submission, &mut conn).await.unwrap();
            notify_on_submission_change.notify_one();
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), rx)
            .await
            .expect("Timed out waiting for HTTP request")
            .expect("Sender dropped without signaling");
    }
}

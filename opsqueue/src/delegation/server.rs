use crate::common::submission::{SubmissionId, SubmissionStatus};
#[cfg(test)]
use crate::db::DBPools;
use crate::db::{Connection, WriterConnection};
use crate::server::interface::Interface;
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
    pool: &DBPools,
    cancellation_token: &CancellationToken,
    delegation_server_url: url::Url,
) -> (Router, tokio::sync::mpsc::UnboundedSender<SubmissionId>) {
    let notify_on_insert = Arc::new(Notify::new());
    let (status_changed_sender, status_changed_receiver) = tokio::sync::mpsc::unbounded_channel();
    let router = ServerState::new(
        delegation_server_url,
        cancellation_token.clone(),
        Interface::new(
            pool.clone(),
            status_changed_sender.clone(),
            status_changed_receiver,
        ),
        notify_on_insert,
    )
    .run_background()
    .build_router();

    (Router::new().nest("/job", router), status_changed_sender)
}

#[derive(Debug, Clone)]
pub struct ServerState {
    interface: Interface,
    cancellation_token: CancellationToken,
    /// Notified when new chunks become available for dispatch (e.g. after unpausing a submission).
    pub notify_on_insert: Arc<Notify>,
    delegation_server_url: url::Url,
    http_client: reqwest::Client,
}

impl ServerState {
    pub fn new(
        delegation_server_url: url::Url,
        cancellation_token: CancellationToken,
        interface: Interface,
        notify_on_insert: Arc<Notify>,
    ) -> Self {
        Self {
            interface,
            cancellation_token,
            notify_on_insert,
            delegation_server_url,
            http_client: reqwest::Client::new(),
        }
    }

    #[must_use]
    pub fn run_background(self) -> Self {
        let state = self.clone();
        let cancellation_token = self.cancellation_token.clone();
        tokio::spawn(async move {
            run_in_background(state, cancellation_token).await.ok();
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
    let mut conn = state.interface.pool.writer_conn().await.map_err(|e| {
        tracing::error!("DB error acquiring writer connection: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    insert_external_task(&mut conn, job.payload.submission_id, &job.task_id)
        .await
        .map_err(|e| {
            tracing::error!("DB error handling delegate event: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    drop(conn);

    state
        .interface
        .unpause_submissions(vec![job.payload.submission_id])
        .await
        .map_err(|e| {
            tracing::error!("DB error handling delegate event: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    state.notify_on_insert.notify_waiters();

    Ok(StatusCode::ACCEPTED)
}

#[tracing::instrument(level = "debug", skip(state))]
async fn job_kill(
    State(state): State<ServerState>,
    Json(task_ids): Json<Vec<String>>,
) -> Result<StatusCode, StatusCode> {
    let mut conn = state.interface.pool.reader_conn().await.map_err(|e| {
        tracing::error!("DB error acquiring writer connection: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let mut submission_ids = Vec::with_capacity(task_ids.len());
    for task_id in &task_ids {
        let submission_id = sqlx::query_scalar!(
            r#"SELECT submission_id AS "submission_id: SubmissionId"
               FROM submissions_external_task
               WHERE task_id = $1"#,
            task_id,
        )
        .fetch_optional(conn.get_inner())
        .await
        .map_err(|e| {
            tracing::error!(%task_id, "DB error looking up task for kill event: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        if let Some(submission_id) = submission_id {
            submission_ids.push(submission_id);
        } else {
            tracing::warn!(%task_id, "Kill event for unknown task_id; ignoring");
        }
    }
    drop(conn);

    state
        .interface
        .cancel_submissions(submission_ids)
        .await
        .map_err(|e| {
            tracing::error!("DB error handling kill event: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

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

const DELEGATION_BACKGROUND_LOOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

async fn run_in_background(
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
            Some(_) = state.interface.wait_for_submission_status_change() => false,
            () = tokio::time::sleep(DELEGATION_BACKGROUND_LOOP_TIMEOUT) => true,
        };
    }

    Ok(())
}

async fn report_submission_status(
    state: &ServerState,
    triggered_by_timeout: bool,
) -> anyhow::Result<()> {
    let out_of_date_tasks = select_out_of_date_tasks(&state.interface).await?;

    if out_of_date_tasks.is_empty() {
        return Ok(());
    }

    if triggered_by_timeout {
        tracing::warn!(
            n_out_of_date_tasks = out_of_date_tasks.len(),
            "Delegation background loop triggered by timeout with pending tasks; \
             possible missing submission status notification"
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
            let conn = state.interface.pool.writer_conn().await?;
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
            let conn = state.interface.pool.writer_conn().await?;
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

async fn select_out_of_date_tasks(interface: &Interface) -> anyhow::Result<Vec<OutOfDateTaskRow>> {
    let external_tasks = {
        let mut conn = interface.pool.reader_conn().await?;
        sqlx::query_as!(
            ExternalTaskRow,
            r#"SELECT
                  task_id,
                  submission_id AS "submission_id: SubmissionId",
                  last_status_sent AS "last_status_sent: DelegatedJobStatus"
              FROM submissions_external_task"#
        )
        .fetch_all(conn.get_inner())
        .await?
    };

    let submission_ids = external_tasks
        .iter()
        .map(|task| task.submission_id)
        .collect::<Vec<_>>();
    let statuses = interface.get_submission_statuses(submission_ids).await?;

    let mut out_of_date_tasks = Vec::new();
    for (task, status) in external_tasks.into_iter().zip(statuses) {
        let status = status.ok_or_else(|| {
            anyhow::anyhow!(
                "Submission {} for external task {} no longer exists",
                task.submission_id,
                task.task_id
            )
        })?;
        let current_status = match status {
            SubmissionStatus::Paused(_) => DelegatedJobStatus::Paused,
            SubmissionStatus::InProgress(_) => DelegatedJobStatus::InProgress,
            SubmissionStatus::Completed(_) => DelegatedJobStatus::Completed,
            SubmissionStatus::Failed(_, _) => DelegatedJobStatus::Failed,
            SubmissionStatus::Cancelled(_) => DelegatedJobStatus::Cancelled,
        };

        if task.last_status_sent != Some(current_status) {
            out_of_date_tasks.push(OutOfDateTaskRow {
                task_id: task.task_id,
                current_status,
            });
        }
    }
    Ok(out_of_date_tasks)
}

#[derive(Debug)]
struct ExternalTaskRow {
    task_id: String,
    submission_id: SubmissionId,
    last_status_sent: Option<DelegatedJobStatus>,
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
    use std::sync::Mutex;
    use tokio::sync::oneshot;
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
        let (app, _status_changed_sender) = app_for_tests(
            &pool,
            &cancellation_token,
            external_server.uri().parse().unwrap(),
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
        let (app, _status_changed_sender) = app_for_tests(
            &pool,
            &cancellation_token,
            external_server.uri().parse().unwrap(),
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
        let (_app, status_changed_sender) = app_for_tests(
            &pool,
            &cancellation_token,
            external_server.uri().parse().unwrap(),
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
            status_changed_sender.send(submission).unwrap();
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
        let (_app, status_changed_sender) = app_for_tests(
            &pool,
            &cancellation_token,
            external_server.uri().parse().unwrap(),
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
            status_changed_sender.send(submission).unwrap();
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
        let (_app, status_changed_sender) = app_for_tests(
            &pool,
            &cancellation_token,
            external_server.uri().parse().unwrap(),
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
            status_changed_sender.send(submission).unwrap();
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
        let (_app, status_changed_sender) = app_for_tests(
            &pool,
            &cancellation_token,
            external_server.uri().parse().unwrap(),
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
            status_changed_sender.send(submission).unwrap();
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), rx)
            .await
            .expect("Timed out waiting for HTTP request")
            .expect("Sender dropped without signaling");
    }
}

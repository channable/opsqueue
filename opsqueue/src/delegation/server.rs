/// An extension for Opsqueue to interact with an external orchestrator. See the top-level README.md
/// for a high-level overview.
///
/// ## Requirements for implementing the `/delegation/submit` endpoint of the external orchestrator
///
/// The external orchestrator should record all events in a request before responding successfully;
/// if the request fails, none of the events should be acted upon, and an error status code should
/// be returned. Opsqueue will resubmit the events at a later point in time.
use crate::common::errors::{E, SubmissionNotFound};
use crate::common::extension::CoreApi;
use crate::common::submission::{SubmissionId, SubmissionStatus};
use crate::db::{Connection, DBPools, WriterConnection};
use crate::tracing::as_dyn_error;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use axum_prometheus::metrics::{counter, histogram};
use either::Either;
use futures::stream::BoxStream;
use itertools::Itertools;
use std::{sync::Arc, time::Duration};
use tokio::select;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
pub(crate) fn app_for_tests(
    cancellation_token: &CancellationToken,
    core_api: CoreApi,
    delegation_server_url: url::Url,
) -> Router {
    let config = crate::config::Config::default();
    let router = ServerState::new(
        cancellation_token.clone(),
        core_api,
        delegation_server_url,
        config.delegation_full_scan_period.into(),
        config.delegation_debounce_period.into(),
        config.delegation_batch_size,
    )
    .run_background()
    .build_router();

    Router::new().nest("/delegation", router)
}

#[derive(Debug, Clone)]
pub struct ServerState {
    cancellation_token: CancellationToken,
    core_api: CoreApi,
    delegation_server_url: url::Url,
    full_scan_period: Duration,
    debounce_period: Duration,
    batch_size: usize,
    http_client: reqwest::Client,
}

impl ServerState {
    #[must_use]
    pub fn new(
        cancellation_token: CancellationToken,
        core_api: CoreApi,
        delegation_server_url: url::Url,
        full_scan_period: Duration,
        debounce_period: Duration,
        batch_size: usize,
    ) -> Self {
        Self {
            cancellation_token,
            core_api,
            delegation_server_url,
            full_scan_period,
            debounce_period,
            batch_size,
            http_client: reqwest::Client::new(),
        }
    }

    #[must_use]
    pub fn run_background(self) -> Self {
        let submission_status_changed_rx = self.core_api.subscribe_submission_status_changes();
        let state = self.clone();
        tokio::spawn(run_in_background(state, submission_status_changed_rx));
        self
    }

    pub fn build_router(self: ServerState) -> Router<()> {
        Router::new()
            .route("/submit", post(submit))
            .with_state(self)
    }
}

/// The status of a delegated Submission.
/// Similar to `SubmissionStatus`, but without the extra fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "TEXT", rename_all = "snake_case")]
enum DelegatedSubmissionStatus {
    Paused,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

/// Delegation events sent from the external orchestrator to Opsqueue.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type", content = "contents")]
enum IncomingDelegationEvent {
    /// Unpauses the submission.
    #[serde(rename = "delegate")]
    Delegate(Vec<DelegatedJob>),
    /// Cancels the submission.
    #[serde(rename = "kill")]
    Kill(Vec<String>),
    /// Currently not implemented. Could be used to implement pausing in the future.
    #[serde(rename = "return")]
    Return(Vec<String>),
}

/// A delegated job (pointing to a submission) as submitted by the external orchestrator.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct DelegatedJob {
    task_id: String,
    payload: DelegatedJobPayload,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct DelegatedJobPayload {
    submission_id: SubmissionId,
}

/// Delegation events sent from Opsqueue to the external orchestrator.
#[derive(Debug, serde::Serialize)]
#[serde(tag = "type", content = "contents")]
enum OutgoingDelegationEvent<'a> {
    /// The job/submission's status was changed to a non-terminal state.
    #[serde(rename = "updated")]
    Updated(Vec<DelegatedJobUpdate<'a>>),
    /// The job/submission's status was changed to a terminal state.
    #[serde(rename = "completed")]
    Completed(Vec<DelegatedJobCompletion<'a>>),
}

#[derive(Debug, serde::Serialize)]
struct DelegatedJobUpdate<'a> {
    task_id: &'a str,
    status: DelegatedJobUpdateStatus,
}

/// The non-terminal statuses as received by the external orchestrator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum DelegatedJobUpdateStatus {
    /// Corresponds to `DelegatedSubmissionStatus::Paused`.
    Queued,
    /// Corresponds to `DelegatedSubmissionStatus::InProgress`.
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
    /// Corresponds to `DelegatedSubmissionStatus::Completed`.
    #[serde(rename = "success")]
    Success,
    /// Corresponds to `DelegatedSubmissionStatus::Failed`
    /// and `DelegatedSubmissionStatus::Cancelled`.
    #[serde(rename = "failure")]
    Failure { failure_reason: FailureReason },
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum FailureReason {
    /// Corresponds to `DelegatedSubmissionStatus::Failed`.
    Unknown,
    /// Corresponds to `DelegatedSubmissionStatus::Cancelled`.
    Forced,
}

#[tracing::instrument(level = "debug", skip(state))]
async fn submit(
    State(state): State<ServerState>,
    Json(events): Json<Vec<IncomingDelegationEvent>>,
) -> Result<StatusCode, StatusCode> {
    let mut conn = state
        .core_api
        .get_db_pools()
        .writer_conn()
        .await
        .map_err(|e| {
            tracing::error!("DB error acquiring writer connection: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    handle_worker_events(&state.core_api, &mut conn, events)
        .await
        .map_err(|e: sqlx::Error| {
            tracing::error!("DB error handling events: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::ACCEPTED)
}

async fn handle_worker_events(
    core_api: &CoreApi,
    conn: &mut impl WriterConnection,
    events: Vec<IncomingDelegationEvent>,
) -> sqlx::Result<()> {
    let core_api = core_api.clone();

    conn.transaction(move |mut tx| {
        Box::pin(async move {
            for event in events {
                match event {
                    IncomingDelegationEvent::Delegate(delegations) => {
                        for delegation in delegations {
                            handle_delegate_event(&core_api, &mut tx, &delegation)
                                .await
                                .inspect_err(|e| {
                                    tracing::error!(
                                        error = as_dyn_error(e),
                                        "Error handling delegate event"
                                    );
                                })?;
                        }
                    }
                    IncomingDelegationEvent::Kill(external_task_ids) => {
                        for external_task_id in external_task_ids {
                            handle_kill_event(&core_api, &mut tx, &external_task_id)
                                .await
                                .map_err(|e| {
                                    tracing::error!(
                                        "Error handling kill event for external_task_id={external_task_id}: {e:?}"
                                    );
                                    e
                                })?;
                        }
                    }
                    IncomingDelegationEvent::Return(_external_task_ids) => {
                        tracing::info!(
                            "Received 'return' delegation event, which is not yet implemented; ignoring."
                        );
                    }
                }
            }

            Ok(())
        })
    }).await
}

#[tracing::instrument(level = "debug", skip(conn))]
async fn handle_delegate_event(
    core_api: &CoreApi,
    conn: &mut impl WriterConnection,
    job: &DelegatedJob,
) -> sqlx::Result<()> {
    let external_task_id = &job.task_id;
    let submission_id = job.payload.submission_id;

    let rows_affected = insert_delegated_submission(conn, submission_id, external_task_id).await?;

    if rows_affected == 0 {
        tracing::debug!(%submission_id, %external_task_id, "Delegated submission was already registered");
    }

    match core_api.unpause_submission(submission_id, conn).await {
        Ok(()) => {}
        Err(E::R(SubmissionNotFound(_))) => {
            // Note that this path could be hit when resubmitting a task with the same
            // submission id.
            //
            // Also see `out_of_date_delegated_submission` for which status we report in these
            // cases (especially the case where the submission was already cleaned up).
            tracing::debug!(%submission_id, "Submission was not in paused state; either already active/completed/failed or already cleaned up.");

            // Send a notification, s.t. the status is promptly reported to the external system.
            core_api.notify_submission_status_changed(submission_id);
        }
        Err(E::L(db_err)) => {
            tracing::error!(%submission_id, "DB error unpausing submission: {db_err:?}");
            return Err(db_err.0);
        }
    }

    Ok(())
}

#[tracing::instrument(level = "debug", skip(conn))]
async fn handle_kill_event(
    core_api: &CoreApi,
    conn: &mut impl WriterConnection,
    external_task_id: &str,
) -> sqlx::Result<()> {
    // Get the submission_id, if it exists.
    let submission_id: Option<SubmissionId> = sqlx::query_scalar!(
        r#"SELECT submission_id AS "submission_id: SubmissionId"
           FROM delegated_submissions
           WHERE external_task_id = $1"#,
        external_task_id,
    )
    .fetch_optional(conn.get_inner())
    .await?;

    let Some(submission_id) = submission_id else {
        tracing::warn!(%external_task_id, "Kill event for unknown external_task_id; ignoring");
        return Ok(());
    };

    match core_api.cancel_submission(submission_id, conn).await {
        Ok(()) => {}
        Err(E::L(db_err)) => {
            tracing::error!(%submission_id, "DB error cancelling submission: {db_err:?}");
            return Err(db_err.0);
        }
        Err(E::R(E::L(SubmissionNotFound(_)))) => {
            tracing::warn!(%submission_id, "Submission not found when attempting to cancel; already gone");
        }
        Err(E::R(E::R(_not_cancelable))) => {
            tracing::debug!(%submission_id, "Submission was already cancelled/failed/completed");
        }
    }

    Ok(())
}

/// An error that occurred while listening for submission changes.
#[derive(Debug)]
enum SubmissionStatusChangeBatchError {
    /// The notification channel was closed, this happens when the server is shutting down.
    Closed,
    /// The notification channel has overflown, and we missed some notifications because of that.
    Lagged,
}

/// Listen for submission status changes.
///
/// Returns the ids of the submissions that changed, collecting notifications for
/// a short period after the first one to batch nearby changes.
///
/// There are two error cases that should be dealt with, see `SubmissionStatusChangeBatchError`.
async fn submission_status_change_batch(
    submission_status_changed: &mut tokio::sync::broadcast::Receiver<SubmissionId>,
    debounce_period: Duration,
) -> Result<Vec<SubmissionId>, SubmissionStatusChangeBatchError> {
    match submission_status_changed.recv().await {
        Ok(submission_id) => {
            let mut submission_ids = vec![submission_id];
            tokio::time::sleep(debounce_period).await;
            loop {
                match submission_status_changed.try_recv() {
                    Ok(submission_id) => submission_ids.push(submission_id),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Closed) => {
                        return Err(SubmissionStatusChangeBatchError::Closed);
                    }
                    Err(TryRecvError::Lagged(_)) => {
                        return Err(SubmissionStatusChangeBatchError::Lagged);
                    }
                }
            }

            Ok(submission_ids)
        }
        Err(RecvError::Closed) => Err(SubmissionStatusChangeBatchError::Closed),
        Err(RecvError::Lagged(_)) => Err(SubmissionStatusChangeBatchError::Lagged),
    }
}

/// What submissions should we check for updates?
enum ReportTrigger {
    /// Check all submissions for updates.
    ///
    /// Used during start-up.
    StartupFullScan,
    /// Check all submissions for updates.
    ///
    /// Used when the notification channel has overflown.
    LaggedFullScan,
    /// Check all submissions for updates.
    ///
    /// Triggered periodically. We don't expect there to be work here, but we have this in-place
    /// as a backup against missing `submission_status_changed.send` calls.
    PeriodicFullScan,
    /// Check specific submissions for updates.
    ///
    /// The happy path. Checks the submissions as sent via `submission_status_changed`.
    Submissions(Vec<SubmissionId>),
}

async fn run_in_background(
    state: ServerState,
    mut submission_status_changed_rx: tokio::sync::broadcast::Receiver<SubmissionId>,
) -> Result<(), ()> {
    tracing::info!(
        "Started delegation background loop. Updates will be sent to {}",
        state.delegation_server_url
    );

    let mut full_scan_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + state.full_scan_period,
        state.full_scan_period,
    );
    full_scan_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut trigger = ReportTrigger::StartupFullScan;

    loop {
        // NOTE: In case this reporting loop is too slow, observe the
        // DELEGATION_REPORTING_LOOP_ITERATIONS_COUNTER and
        // DELEGATION_TASKS_REPORTED_PER_ITERATION_HISTOGRAM metrics.
        //
        // Informed by those, try:
        //  - Increasing the full-scan period, to amortize the cost over time.
        //  - Increase the capacity of the `submission_status_changed` channel, to avoid lagged
        //    full scans.
        //  - Increase the debounce period, trading latency for throughput.

        match report_submission_status(&state, trigger).await {
            Ok(()) => {}
            Err(e) => tracing::error!("Error in delegation background loop: {e:?}"),
        }

        trigger = select! {
            () = state.cancellation_token.cancelled() => break,
            res = submission_status_change_batch(&mut submission_status_changed_rx, state.debounce_period) => {
                match res {
                    Ok(submission_ids) => ReportTrigger::Submissions(submission_ids),
                    Err(SubmissionStatusChangeBatchError::Lagged) => ReportTrigger::LaggedFullScan,
                    Err(SubmissionStatusChangeBatchError::Closed) => break,
                }
            },
            _ = full_scan_interval.tick() => ReportTrigger::PeriodicFullScan
        };
    }

    Ok(())
}

fn increment_delegation_reporting_loop_iterations(trigger: &ReportTrigger) {
    let trigger_label = match trigger {
        ReportTrigger::StartupFullScan => "startup_full_scan",
        ReportTrigger::LaggedFullScan => "lagged_full_scan",
        ReportTrigger::PeriodicFullScan => "periodic_full_scan",
        ReportTrigger::Submissions(_) => "submissions",
    };

    counter!(
        crate::prometheus::DELEGATION_REPORTING_LOOP_ITERATIONS_COUNTER,
        &[("trigger", trigger_label)]
    )
    .increment(1);
}

/// Check for and report submission status updates to the external orchestrator.
#[allow(clippy::cast_precision_loss)]
async fn report_submission_status(
    state: &ServerState,
    trigger: ReportTrigger,
) -> Result<(), E<sqlx::Error, reqwest::Error>> {
    use futures::{StreamExt, TryStreamExt};

    increment_delegation_reporting_loop_iterations(&trigger);

    let triggered_by_timeout = matches!(trigger, ReportTrigger::PeriodicFullScan);
    let update_specific_submissions = match trigger {
        ReportTrigger::Submissions(submission_ids) => Some(submission_ids),
        ReportTrigger::StartupFullScan
        | ReportTrigger::LaggedFullScan
        | ReportTrigger::PeriodicFullScan => None,
    };
    let mut conn_0 = state
        .core_api
        .get_db_pools()
        .reader_conn()
        .await
        .map_err(E::L)?;
    let conn_1 = Arc::new(tokio::sync::Mutex::new(
        state
            .core_api
            .get_db_pools()
            .reader_conn()
            .await
            .map_err(E::L)?,
    ));
    let core_api = Arc::new(state.core_api.clone());

    let out_of_date_tasks = {
        select_delegated_submissions(&mut conn_0, update_specific_submissions)
            .try_filter_map({
                move |task| {
                    let conn_1 = conn_1.clone();
                    let core_api = core_api.clone();
                    async move {
                        let mut conn_1 = conn_1.lock().await;
                        let status = core_api
                            .submission_status(task.submission_id, &mut *conn_1)
                            .await
                            .map_err(|err| err.0)?;
                        Ok(out_of_date_delegated_submission(task, (&status).into()))
                    }
                }
            })
            .try_chunks(state.batch_size)
    };

    let mut task_count = 0;

    tokio::pin!(out_of_date_tasks);

    while let Some(batch_or_error) = out_of_date_tasks.next().await {
        let batch = batch_or_error.map_err(|err| E::L(err.1))?;

        let batch_len = batch.len();

        send_status_updates(state, batch.iter())
            .await
            .map_err(E::R)?;

        update_statuses_in_db(state.core_api.get_db_pools(), batch)
            .await
            .map_err(E::L)?;

        task_count += batch_len;
    }

    histogram!(crate::prometheus::DELEGATION_TASKS_REPORTED_PER_ITERATION_HISTOGRAM)
        .record(task_count as f64);

    if task_count > 0 && triggered_by_timeout {
        tracing::warn!(
            n_out_of_date_tasks = task_count,
            "Delegation background loop was triggered by a periodic full scan. Barring race \
             conditions we don't expect status changes. However, there were some status changes.\
             Elevated levels of this warning could indicate a missing\
             `submission_status_changed.send` call."
        );
    }

    Ok(())
}

async fn insert_delegated_submission(
    conn: &mut impl Connection,
    submission_id: SubmissionId,
    external_task_id: &str,
) -> sqlx::Result<u64> {
    let rows_affected = sqlx::query!(
        r#"INSERT INTO delegated_submissions (submission_id, external_task_id, last_status_sent)
           SELECT $1 AS submission_id, $2 AS external_task_id, NULL AS last_status_sent
           WHERE NOT EXISTS (
            SELECT TRUE
            FROM delegated_submissions
            WHERE submission_id = $1 AND external_task_id = $2
           )"#,
        submission_id,
        external_task_id,
    )
    .execute(conn.get_inner())
    .await?
    .rows_affected();

    Ok(rows_affected)
}

#[derive(Debug)]
struct DelegatedSubmission {
    submission_id: SubmissionId,
    external_task_id: String,
    last_status_sent: Option<DelegatedSubmissionStatus>,
}

fn select_delegated_submissions(
    conn: &mut impl Connection,
    submission_ids: Option<Vec<SubmissionId>>,
) -> BoxStream<'_, sqlx::Result<DelegatedSubmission>> {
    if let Some(submission_ids) = submission_ids {
        sqlx::query_as!(
            DelegatedSubmission,
            r#"SELECT
                    submission_id as "submission_id!: SubmissionId",
                    external_task_id,
                    last_status_sent as "last_status_sent: DelegatedSubmissionStatus"
                FROM delegated_submissions as t
                WHERE t.submission_id IN (SELECT value FROM json_each($1))
            "#,
            serde_json::to_string(&submission_ids).expect("Failed to serialize ids")
        )
        .fetch(conn.get_inner())
    } else {
        sqlx::query_as!(
            DelegatedSubmission,
            r#"SELECT
                    submission_id as "submission_id!: SubmissionId",
                    external_task_id,
                    last_status_sent as "last_status_sent: DelegatedSubmissionStatus"
                FROM delegated_submissions as t
            "#
        )
        .fetch(conn.get_inner())
    }
}

struct OutOfDateDelegatedSubmission {
    external_task_id: String,
    current_status: DelegatedSubmissionStatus,
}

fn out_of_date_delegated_submission(
    task: DelegatedSubmission,
    status: Option<&SubmissionStatus>,
) -> Option<OutOfDateDelegatedSubmission> {
    let current_status = match status {
        Some(SubmissionStatus::Paused(_)) => DelegatedSubmissionStatus::Paused,
        Some(SubmissionStatus::InProgress(_)) => DelegatedSubmissionStatus::InProgress,
        Some(SubmissionStatus::Completed(_)) => DelegatedSubmissionStatus::Completed,
        Some(SubmissionStatus::Failed(_, _)) => DelegatedSubmissionStatus::Failed,
        Some(SubmissionStatus::Cancelled(_)) => DelegatedSubmissionStatus::Cancelled,
        None => {
            let external_task_id = &task.external_task_id;
            let submission_id = &task.submission_id;
            // Log to Sentry, for visibility.
            tracing::error!(%submission_id, %external_task_id,
                "A submission was delegated, but we could not find its submission. This could be a\
                resubmitted task, where we already cleaned up the corresponding submission after \
                failure/completion. Assuming it completed successfully. If this submission was\
                manually resubmitted/redelegated, consider regenerating the full submission\
                instead.");
            DelegatedSubmissionStatus::Completed
        }
    };
    (task.last_status_sent != Some(current_status)).then_some(OutOfDateDelegatedSubmission {
        external_task_id: task.external_task_id,
        current_status,
    })
}

/// Convert an `OutOfDateDelegatedSubmission` into a `DelegatedJobUpdate` or
/// `DelegatedJobCompletion` we can send to the external orchestrator.
fn status_update(
    task: &OutOfDateDelegatedSubmission,
) -> Either<DelegatedJobUpdate<'_>, DelegatedJobCompletion<'_>> {
    match task.current_status {
        DelegatedSubmissionStatus::Paused => Either::Left(DelegatedJobUpdate {
            task_id: &task.external_task_id,
            status: DelegatedJobUpdateStatus::Queued,
        }),
        DelegatedSubmissionStatus::InProgress => Either::Left(DelegatedJobUpdate {
            task_id: &task.external_task_id,
            status: DelegatedJobUpdateStatus::Running,
        }),
        DelegatedSubmissionStatus::Completed => Either::Right(DelegatedJobCompletion {
            task_id: &task.external_task_id,
            completion: DelegatedJobCompletionStatus::Success,
        }),
        DelegatedSubmissionStatus::Failed => Either::Right(DelegatedJobCompletion {
            task_id: &task.external_task_id,
            completion: DelegatedJobCompletionStatus::Failure {
                failure_reason: FailureReason::Unknown,
            },
        }),
        DelegatedSubmissionStatus::Cancelled => Either::Right(DelegatedJobCompletion {
            task_id: &task.external_task_id,
            completion: DelegatedJobCompletionStatus::Failure {
                failure_reason: FailureReason::Forced,
            },
        }),
    }
}

async fn update_statuses_in_db(
    pool: &DBPools,
    tasks: Vec<OutOfDateDelegatedSubmission>,
) -> sqlx::Result<()> {
    pool.writer_conn()
        .await?
        .transaction(move |mut tx| {
            Box::pin(async move {
                for task in tasks {
                    match task.current_status {
                        DelegatedSubmissionStatus::Paused
                        | DelegatedSubmissionStatus::InProgress => {
                            update_last_status_sent(&mut tx, &task).await?;
                        }
                        DelegatedSubmissionStatus::Completed
                        | DelegatedSubmissionStatus::Failed
                        | DelegatedSubmissionStatus::Cancelled => {
                            delete_external_task(&mut tx, &task).await?;
                        }
                    }
                }
                Ok(())
            })
        })
        .await
}

async fn update_last_status_sent(
    mut conn: impl WriterConnection,
    task: &OutOfDateDelegatedSubmission,
) -> sqlx::Result<()> {
    sqlx::query!(
        "UPDATE delegated_submissions SET last_status_sent = $1 WHERE external_task_id = $2",
        task.current_status,
        task.external_task_id,
    )
    .execute(conn.get_inner())
    .await?;

    Ok(())
}

async fn delete_external_task(
    mut conn: impl WriterConnection,
    task: &OutOfDateDelegatedSubmission,
) -> sqlx::Result<()> {
    sqlx::query!(
        "DELETE FROM delegated_submissions WHERE external_task_id = $1",
        task.external_task_id,
    )
    .execute(conn.get_inner())
    .await?;

    Ok(())
}

async fn send_status_updates(
    state: &ServerState,
    events: impl Iterator<Item = &OutOfDateDelegatedSubmission>,
) -> reqwest::Result<()> {
    let (updates, completions): (Vec<_>, Vec<_>) = events.partition_map(status_update);
    let events = vec![
        OutgoingDelegationEvent::Updated(updates),
        OutgoingDelegationEvent::Completed(completions),
    ];
    state
        .http_client
        .post(
            state
                .delegation_server_url
                .join("/delegation/submit")
                .unwrap(),
        )
        .json(&events)
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
    use crate::common::extension::CoreApi;
    use crate::common::submission::db::{
        count_submissions, count_submissions_cancelled, count_submissions_paused,
        insert_submission_from_chunks,
    };
    use crate::common::submission::{InitialSubmissionStatus, SubmissionId, SubmissionStatus};
    use crate::db::{Connection, DBPools};
    use crate::delegation::server::{
        DelegatedJob, DelegatedJobPayload, ReportTrigger, ServerState, app_for_tests,
        handle_delegate_event, handle_kill_event, insert_delegated_submission,
        report_submission_status, submission_status_change_batch,
    };
    use axum::http::Request;
    use axum::{Router, body::Body};
    use http::{StatusCode, header};
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use tokio::sync::{Notify, broadcast, oneshot};
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt;
    use wiremock::matchers::{body_json, body_partial_json, method, path};
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

    async fn count_delegated_submissions(mut db: impl Connection) -> sqlx::Result<u64> {
        let count = sqlx::query_scalar!("SELECT COUNT(*) as count FROM delegated_submissions;")
            .fetch_one(db.get_inner())
            .await?;
        Ok(u64::try_from(count).expect("COUNT(*) is always non-negative"))
    }

    struct TestContext {
        pool: DBPools,
        external_server: MockServer,
        core_api: CoreApi,
        submission_status_changed: broadcast::Sender<SubmissionId>,
        app: Router,
    }

    impl TestContext {
        async fn new(
            pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
            conn_opts: sqlx::sqlite::SqliteConnectOptions,
        ) -> Self {
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
            let (submission_status_changed, _) = broadcast::channel(128);
            let core_api = CoreApi::new(
                pool.clone(),
                Arc::new(Notify::new()),
                submission_status_changed.clone(),
            );
            let cancellation_token = CancellationToken::new();
            let app = app_for_tests(
                &cancellation_token,
                core_api.clone(),
                external_server.uri().parse().unwrap(),
            );
            Self {
                pool,
                external_server,
                core_api,
                submission_status_changed,
                app,
            }
        }

        async fn post_worker_events(&self, events: serde_json::Value) {
            let request = Request::builder()
                .uri("/delegation/submit")
                .method("POST")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(events.to_string()))
                .unwrap();
            let response = self.app.clone().oneshot(request).await.unwrap();
            assert_eq!(
                response.status(),
                StatusCode::ACCEPTED,
                "request failed: {response:?}"
            );
        }
    }

    async fn wait_for_update(rx: oneshot::Receiver<()>) {
        tokio::time::timeout(std::time::Duration::from_secs(2), rx)
            .await
            .expect("Timed out waiting for HTTP request")
            .expect("Sender dropped without signaling");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_delegate_event_for_paused_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let mut conn = pool.writer_conn().await.unwrap();
        let submission_id = insert_submission_from_chunks(
            None,
            vec![Some("foo".into())],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            InitialSubmissionStatus::Paused,
            &mut conn,
        )
        .await
        .unwrap();
        assert!(matches!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            Some(SubmissionStatus::Paused(_))
        ));

        let job = DelegatedJob {
            task_id: "test".to_owned(),
            payload: DelegatedJobPayload { submission_id },
        };
        let mut notifications = core_api.subscribe_submission_status_changes();
        handle_delegate_event(&core_api, &mut conn, &job)
            .await
            .unwrap();

        assert!(matches!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            Some(SubmissionStatus::InProgress(_))
        ));
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert_eq!(notifications.try_recv().unwrap(), submission_id);

        // Test for idempotency.
        handle_delegate_event(&core_api, &mut conn, &job)
            .await
            .unwrap();
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert_eq!(notifications.try_recv().unwrap(), submission_id);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_delegate_event_for_in_progress_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let mut conn = pool.writer_conn().await.unwrap();
        let submission_id = insert_submission_from_chunks(
            None,
            vec![Some("foo".into())],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            InitialSubmissionStatus::InProgress,
            &mut conn,
        )
        .await
        .unwrap();
        let before = core_api
            .submission_status(submission_id, &mut conn)
            .await
            .unwrap();
        assert!(matches!(before, Some(SubmissionStatus::InProgress(_))));

        let job = DelegatedJob {
            task_id: "test".to_owned(),
            payload: DelegatedJobPayload { submission_id },
        };
        let mut notifications = core_api.subscribe_submission_status_changes();
        handle_delegate_event(&core_api, &mut conn, &job)
            .await
            .unwrap();

        assert_eq!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            before
        );
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert_eq!(notifications.try_recv().unwrap(), submission_id);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_delegate_event_for_completed_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let mut conn = pool.writer_conn().await.unwrap();
        let submission_id = insert_submission_from_chunks(
            None,
            vec![],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            InitialSubmissionStatus::InProgress,
            &mut conn,
        )
        .await
        .unwrap();
        let before = core_api
            .submission_status(submission_id, &mut conn)
            .await
            .unwrap();
        assert!(matches!(before, Some(SubmissionStatus::Completed(_))));

        let job = DelegatedJob {
            task_id: "test".to_owned(),
            payload: DelegatedJobPayload { submission_id },
        };
        let mut notifications = core_api.subscribe_submission_status_changes();
        handle_delegate_event(&core_api, &mut conn, &job)
            .await
            .unwrap();

        assert_eq!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            before
        );
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert_eq!(notifications.try_recv().unwrap(), submission_id);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_delegate_event_for_failed_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(
            pool.clone(),
            Arc::new(Notify::new()),
            status_changed.clone(),
        );
        let mut conn = pool.writer_conn().await.unwrap();
        let submission_id = insert_submission_from_chunks(
            None,
            vec![Some("foo".into())],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            InitialSubmissionStatus::InProgress,
            &mut conn,
        )
        .await
        .unwrap();
        retry_or_fail_chunk(
            (submission_id, ChunkIndex::zero()).into(),
            "error".to_owned(),
            &mut conn,
            0,
            &status_changed,
        )
        .await
        .unwrap();
        let before = core_api
            .submission_status(submission_id, &mut conn)
            .await
            .unwrap();
        assert!(matches!(before, Some(SubmissionStatus::Failed(_, _))));

        let job = DelegatedJob {
            task_id: "test".to_owned(),
            payload: DelegatedJobPayload { submission_id },
        };
        let mut notifications = core_api.subscribe_submission_status_changes();
        handle_delegate_event(&core_api, &mut conn, &job)
            .await
            .unwrap();

        assert_eq!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            before
        );
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert_eq!(notifications.try_recv().unwrap(), submission_id);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_delegate_event_for_cancelled_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let mut conn = pool.writer_conn().await.unwrap();
        let submission_id = insert_submission_from_chunks(
            None,
            vec![Some("foo".into())],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            InitialSubmissionStatus::InProgress,
            &mut conn,
        )
        .await
        .unwrap();
        core_api
            .cancel_submission(submission_id, &mut conn)
            .await
            .unwrap();
        let before = core_api
            .submission_status(submission_id, &mut conn)
            .await
            .unwrap();
        assert!(matches!(before, Some(SubmissionStatus::Cancelled(_))));

        let job = DelegatedJob {
            task_id: "test".to_owned(),
            payload: DelegatedJobPayload { submission_id },
        };
        let mut notifications = core_api.subscribe_submission_status_changes();
        handle_delegate_event(&core_api, &mut conn, &job)
            .await
            .unwrap();

        assert_eq!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            before
        );
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert_eq!(notifications.try_recv().unwrap(), submission_id);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_delegate_event_for_missing_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let mut conn = pool.writer_conn().await.unwrap();

        let submission_id = SubmissionId::new();
        let job = DelegatedJob {
            task_id: "test".to_owned(),
            payload: DelegatedJobPayload { submission_id },
        };
        let mut notifications = core_api.subscribe_submission_status_changes();
        handle_delegate_event(&core_api, &mut conn, &job)
            .await
            .unwrap();

        assert_eq!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            None
        );
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert_eq!(notifications.try_recv().unwrap(), submission_id);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_kill_event_for_paused_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let mut conn = pool.writer_conn().await.unwrap();
        let submission_id = insert_submission_from_chunks(
            None,
            vec![Some("foo".into())],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            InitialSubmissionStatus::Paused,
            &mut conn,
        )
        .await
        .unwrap();
        assert!(matches!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            Some(SubmissionStatus::Paused(_))
        ));
        insert_delegated_submission(&mut conn, submission_id, "test")
            .await
            .unwrap();

        let mut notifications = core_api.subscribe_submission_status_changes();
        handle_kill_event(&core_api, &mut conn, "test")
            .await
            .unwrap();

        assert!(matches!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            Some(SubmissionStatus::Cancelled(_))
        ));
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert_eq!(notifications.try_recv().unwrap(), submission_id);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_kill_event_for_in_progress_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let mut conn = pool.writer_conn().await.unwrap();
        let submission_id = insert_submission_from_chunks(
            None,
            vec![Some("foo".into())],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            InitialSubmissionStatus::InProgress,
            &mut conn,
        )
        .await
        .unwrap();
        assert!(matches!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            Some(SubmissionStatus::InProgress(_))
        ));
        insert_delegated_submission(&mut conn, submission_id, "test")
            .await
            .unwrap();

        let mut notifications = core_api.subscribe_submission_status_changes();
        handle_kill_event(&core_api, &mut conn, "test")
            .await
            .unwrap();

        assert!(matches!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            Some(SubmissionStatus::Cancelled(_))
        ));
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert_eq!(notifications.try_recv().unwrap(), submission_id);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_kill_event_for_completed_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let mut conn = pool.writer_conn().await.unwrap();
        let submission_id = insert_submission_from_chunks(
            None,
            vec![],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            InitialSubmissionStatus::InProgress,
            &mut conn,
        )
        .await
        .unwrap();
        let before = core_api
            .submission_status(submission_id, &mut conn)
            .await
            .unwrap();
        assert!(matches!(before, Some(SubmissionStatus::Completed(_))));
        insert_delegated_submission(&mut conn, submission_id, "test")
            .await
            .unwrap();

        let notifications = core_api.subscribe_submission_status_changes();
        handle_kill_event(&core_api, &mut conn, "test")
            .await
            .unwrap();

        assert_eq!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            before
        );
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert!(notifications.is_empty());
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_kill_event_for_failed_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(
            pool.clone(),
            Arc::new(Notify::new()),
            status_changed.clone(),
        );
        let mut conn = pool.writer_conn().await.unwrap();
        let submission_id = insert_submission_from_chunks(
            None,
            vec![Some("foo".into())],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            InitialSubmissionStatus::InProgress,
            &mut conn,
        )
        .await
        .unwrap();
        retry_or_fail_chunk(
            (submission_id, ChunkIndex::zero()).into(),
            "error".to_owned(),
            &mut conn,
            0,
            &status_changed,
        )
        .await
        .unwrap();
        let before = core_api
            .submission_status(submission_id, &mut conn)
            .await
            .unwrap();
        assert!(matches!(before, Some(SubmissionStatus::Failed(_, _))));
        insert_delegated_submission(&mut conn, submission_id, "test")
            .await
            .unwrap();

        let notifications = core_api.subscribe_submission_status_changes();
        handle_kill_event(&core_api, &mut conn, "test")
            .await
            .unwrap();

        assert_eq!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            before
        );
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert!(notifications.is_empty());
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_kill_event_for_cancelled_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let mut conn = pool.writer_conn().await.unwrap();
        let submission_id = insert_submission_from_chunks(
            None,
            vec![Some("foo".into())],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            InitialSubmissionStatus::InProgress,
            &mut conn,
        )
        .await
        .unwrap();
        core_api
            .cancel_submission(submission_id, &mut conn)
            .await
            .unwrap();
        let before = core_api
            .submission_status(submission_id, &mut conn)
            .await
            .unwrap();
        assert!(matches!(before, Some(SubmissionStatus::Cancelled(_))));
        insert_delegated_submission(&mut conn, submission_id, "test")
            .await
            .unwrap();

        let notifications = core_api.subscribe_submission_status_changes();
        handle_kill_event(&core_api, &mut conn, "test")
            .await
            .unwrap();

        assert_eq!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            before
        );
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert!(notifications.is_empty());
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_kill_event_for_missing_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let mut conn = pool.writer_conn().await.unwrap();
        let submission_id = SubmissionId::new();
        insert_delegated_submission(&mut conn, submission_id, "test")
            .await
            .unwrap();

        let notifications = core_api.subscribe_submission_status_changes();
        handle_kill_event(&core_api, &mut conn, "test")
            .await
            .unwrap();

        assert_eq!(
            core_api
                .submission_status(submission_id, &mut conn)
                .await
                .unwrap(),
            None
        );
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
        assert!(notifications.is_empty());
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn handle_kill_event_for_unknown_task(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let mut conn = pool.writer_conn().await.unwrap();

        let notifications = core_api.subscribe_submission_status_changes();
        handle_kill_event(&core_api, &mut conn, "unknown")
            .await
            .unwrap();

        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 0);
        assert!(notifications.is_empty());
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn report_submission_status_for_missing_submission(db: sqlx::SqlitePool) {
        let pool = DBPools::from_test_pool(&db);
        let (status_changed, _) = broadcast::channel(16);
        let core_api = CoreApi::new(pool.clone(), Arc::new(Notify::new()), status_changed);
        let submission_id = SubmissionId::new();
        {
            let mut conn = pool.writer_conn().await.unwrap();
            assert_eq!(
                core_api
                    .submission_status(submission_id, &mut conn)
                    .await
                    .unwrap(),
                None
            );
            insert_delegated_submission(&mut conn, submission_id, "test")
                .await
                .unwrap();
        }

        let external_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/delegation/submit"))
            .and(body_json(json!([
                {"type": "updated", "contents": []},
                {"type": "completed", "contents": [
                    {"task_id": "test", "completion": {"status": "success"}}
                ]}
            ])))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&external_server)
            .await;

        let config = crate::config::Config::default();
        let state = ServerState::new(
            CancellationToken::new(),
            core_api,
            external_server.uri().parse().unwrap(),
            config.delegation_full_scan_period.into(),
            config.delegation_debounce_period.into(),
            config.delegation_batch_size,
        );
        report_submission_status(&state, ReportTrigger::Submissions(vec![submission_id]))
            .await
            .unwrap();

        external_server.verify().await;
        let mut conn = pool.reader_conn().await.unwrap();
        assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn submission_changes_are_batched_during_debounce_period() {
        let (sender, mut receiver) = broadcast::channel(16);
        let first = SubmissionId::new();
        let second = SubmissionId::new();
        sender.send(first).unwrap();

        let debounce_period = std::time::Duration::from_millis(100);
        let mut batch = Box::pin(submission_status_change_batch(
            &mut receiver,
            debounce_period,
        ));
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs_f64(0.5 * debounce_period.as_secs_f64()),
                &mut batch
            )
            .await
            .is_err()
        );

        sender.send(second).unwrap();
        let ids = tokio::time::timeout(
            std::time::Duration::from_secs_f64(4.0 * debounce_period.as_secs_f64()),
            batch,
        )
        .await
        .expect("Timed out waiting for batched notifications")
        .unwrap();
        assert_eq!(ids, vec![first, second]);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_job_delegation(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let test = TestContext::new(pool_opts, conn_opts).await;

        let submission = {
            let mut conn = test.pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                InitialSubmissionStatus::Paused,
                &mut conn,
            )
            .await
            .unwrap()
        };

        {
            let mut conn = test.pool.reader_conn().await.unwrap();
            assert_eq!(count_submissions_paused(&mut conn).await.unwrap(), 1);
            assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 0);
        }

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("POST"))
            .and(path("/delegation/submit"))
            .and(body_partial_json(
                json!([{"type": "updated", "contents": [{"task_id": "test", "status": "running"}]}, {"type": "completed", "contents": []}]),
            ))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&test.external_server)
            .await;

        test.post_worker_events(json!([{"type": "delegate", "contents": [DelegatedJob {
            task_id: "test".to_string(),
            payload: DelegatedJobPayload { submission_id: submission },
        }]}]))
        .await;

        wait_for_update(rx).await;

        {
            let mut conn = test.pool.reader_conn().await.unwrap();
            assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 1);
            assert_eq!(count_submissions(&mut conn).await.unwrap(), 1);
        }
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_job_kill(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let test = TestContext::new(pool_opts, conn_opts).await;

        {
            let mut conn = test.pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            let submission = insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                InitialSubmissionStatus::Paused,
                &mut conn,
            )
            .await
            .unwrap();

            insert_delegated_submission(&mut conn, submission, "test")
                .await
                .unwrap();
        };

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("POST"))
            .and(path("/delegation/submit"))
            .and(body_partial_json(json!([{"type": "updated", "contents": []}, {"type": "completed", "contents": [{"task_id": "test", "completion": {"status": "failure", "failure_reason": "forced"}}]}])))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&test.external_server)
            .await;

        test.post_worker_events(json!([{"type": "kill", "contents": ["test"]}]))
            .await;

        {
            let mut conn = test.pool.reader_conn().await.unwrap();
            assert_eq!(count_submissions_cancelled(&mut conn).await.unwrap(), 1);
            assert_eq!(count_submissions(&mut conn).await.unwrap(), 0);
        }

        wait_for_update(rx).await;

        // Wait for background loop to remove delegated submission;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        {
            let mut conn = test.pool.reader_conn().await.unwrap();
            assert_eq!(count_delegated_submissions(&mut conn).await.unwrap(), 0);
        }
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_unpause_update(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let test = TestContext::new(pool_opts, conn_opts).await;

        let submission = {
            let mut conn = test.pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            let submission = insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                InitialSubmissionStatus::Paused,
                &mut conn,
            )
            .await
            .unwrap();

            insert_delegated_submission(&mut conn, submission, "test")
                .await
                .unwrap();

            submission
        };

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("POST"))
            .and(path("/delegation/submit"))
            .and(body_partial_json(
                json!([{"type": "updated", "contents": [{"task_id": "test", "status": "running"}]}, {"type": "completed", "contents": []}]),
            ))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&test.external_server)
            .await;

        {
            let mut conn = test.pool.writer_conn().await.unwrap();
            test.core_api
                .unpause_submission(submission, &mut conn)
                .await
                .unwrap();
        }

        wait_for_update(rx).await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_complete_update(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let test = TestContext::new(pool_opts, conn_opts).await;

        let submission = {
            let mut conn = test.pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            let submission = insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                InitialSubmissionStatus::InProgress,
                &mut conn,
            )
            .await
            .unwrap();

            insert_delegated_submission(&mut conn, submission, "test")
                .await
                .unwrap();

            submission
        };

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("POST"))
            .and(path("/delegation/submit"))
            .and(body_partial_json(
                json!([{"type": "updated", "contents": []}, {"type": "completed", "contents": [{"task_id": "test", "completion": {"status": "success"}}]}]),
            ))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&test.external_server)
            .await;

        {
            let mut conn = test.pool.writer_conn().await.unwrap();
            complete_chunk(
                (submission, ChunkIndex::zero()).into(),
                None,
                &mut conn,
                &test.submission_status_changed,
            )
            .await
            .unwrap();
        }

        wait_for_update(rx).await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_fail_update(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let test = TestContext::new(pool_opts, conn_opts).await;

        let submission = {
            let mut conn = test.pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            let submission = insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                InitialSubmissionStatus::InProgress,
                &mut conn,
            )
            .await
            .unwrap();

            insert_delegated_submission(&mut conn, submission, "test")
                .await
                .unwrap();

            submission
        };

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("POST"))
            .and(path("/delegation/submit"))
            .and(body_partial_json(json!([{"type": "updated", "contents": []}, {"type": "completed", "contents": [{"task_id": "test", "completion": {"status": "failure", "failure_reason": "unknown"}}]}])))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&test.external_server)
            .await;

        {
            let mut conn = test.pool.writer_conn().await.unwrap();
            retry_or_fail_chunk(
                (submission, ChunkIndex::zero()).into(),
                "extreme error".to_owned(),
                &mut conn,
                0,
                &test.submission_status_changed,
            )
            .await
            .unwrap();
        }

        wait_for_update(rx).await;
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_cancel_update(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let test = TestContext::new(pool_opts, conn_opts).await;

        let submission = {
            let mut conn = test.pool.writer_conn().await.unwrap();

            let chunks_contents = vec![Some("foo".into())];
            let submission = insert_submission_from_chunks(
                None,
                chunks_contents.clone(),
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                InitialSubmissionStatus::InProgress,
                &mut conn,
            )
            .await
            .unwrap();

            insert_delegated_submission(&mut conn, submission, "test")
                .await
                .unwrap();

            submission
        };

        let (tx, rx) = oneshot::channel::<()>();
        Mock::given(method("POST"))
            .and(path("/delegation/submit"))
            .and(body_partial_json(json!([{"type": "updated", "contents": []}, {"type": "completed", "contents": [{"task_id": "test", "completion": {"status": "failure", "failure_reason": "forced"}}]}])))
            .respond_with(SignalResponder::new(tx, ResponseTemplate::new(202)))
            .expect(1)
            .mount(&test.external_server)
            .await;

        {
            let mut conn = test.pool.writer_conn().await.unwrap();
            test.core_api
                .cancel_submission(submission, &mut conn)
                .await
                .unwrap();
        }

        wait_for_update(rx).await;
    }
}

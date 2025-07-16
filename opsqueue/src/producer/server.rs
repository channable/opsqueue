use std::sync::Arc;

use crate::common::submission::{self, SubmissionId};
use crate::db::DBPools;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::sync::Notify;

use super::common::{ChunkContents, InsertSubmission};

pub async fn serve_for_tests(database_pool: DBPools, server_addr: Box<str>) {
    ServerState::new(database_pool, Arc::new(Notify::new()))
        .serve_for_tests(server_addr)
        .await;
}

#[derive(Debug, Clone)]
pub struct ServerState {
    pool: DBPools,
    notify_on_insert: Arc<Notify>,
}

impl ServerState {
    pub fn new(pool: DBPools, notify_on_insert: Arc<Notify>) -> Self {
        ServerState {
            pool,
            notify_on_insert,
        }
    }
    pub async fn serve_for_tests(self, server_addr: Box<str>) {
        let app = Router::new().nest("/producer", self.build_router());

        let listener = tokio::net::TcpListener::bind(&*server_addr)
            .await
            .expect("Failed to bind to producer server address");

        tracing::info!("Producer HTTP server listening at {server_addr}...");
        axum::serve(listener, app)
            .await
            .expect("Failed to start producer server");
    }

    pub fn build_router(self) -> Router<()> {
        Router::new()
            .route("/submissions", post(insert_submission))
            .route(
                "/submissions/count_completed",
                get(submissions_count_completed),
            )
            .route("/submissions/count", get(submissions_count))
            .route(
                "/submissions/lookup_id_by_prefix/:prefix",
                get(lookup_submission_id_by_prefix),
            )
            .route("/submissions/:submission_id", get(submission_status))
            .route("/version", get(crate::server::version_endpoint)) // We're also exposing it here so the producer client can view it
            .with_state(self)
    }
}

// Make our own error that wraps `anyhow::Error`.
struct ServerError(anyhow::Error);

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        tracing::error!("Producer Server Error {:?}", self.0);

        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(format!("{:?}", self.0)),
        )
            .into_response()
    }
}

impl<E> From<E> for ServerError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

async fn submission_status(
    State(state): State<ServerState>,
    Path(submission_id): Path<SubmissionId>,
) -> Result<Json<Option<submission::SubmissionStatus>>, ServerError> {
    let mut conn = state.pool.reader_conn().await?;
    let status = submission::db::submission_status(submission_id, &mut conn).await?;
    Ok(Json(status))
}

async fn lookup_submission_id_by_prefix(
    State(state): State<ServerState>,
    Path(prefix): Path<String>,
) -> Result<Json<Option<SubmissionId>>, ServerError> {
    let mut conn = state.pool.reader_conn().await?;
    let submission_id = submission::db::lookup_id_by_prefix(&prefix, &mut conn).await?;
    Ok(Json(submission_id))
}

#[tracing::instrument(level = "debug", skip(state))]
async fn insert_submission(
    State(state): State<ServerState>,
    Json(request): Json<InsertSubmission>,
) -> Result<Json<SubmissionId>, ServerError> {
    let mut conn = state.pool.writer_conn().await?;
    let (prefix, chunk_contents) = match request.chunk_contents {
        ChunkContents::Direct { contents } => (None, contents),
        ChunkContents::SeeObjectStorage { prefix, count } => {
            let count = u64::from(count);
            (Some(prefix), (0..count).map(|_index| None).collect())
        }
    };
    let submission_id = submission::db::insert_submission_from_chunks(
        prefix,
        chunk_contents,
        request.metadata,
        request.strategic_metadata,
        request.chunk_size.unwrap_or_default(),
        &mut conn,
    )
    .await?;

    // We've done a new insert! Let's tell any waiting consumers!
    state.notify_on_insert.notify_waiters();

    Ok(Json(submission_id))
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct InsertSubmissionResponse {
    pub id: SubmissionId,
}

async fn submissions_count(State(state): State<ServerState>) -> Result<Json<u32>, ServerError> {
    let mut conn = state.pool.reader_conn().await?;
    let count = submission::db::count_submissions(&mut conn).await?;
    Ok(Json(count.try_into()?))
}

async fn submissions_count_completed(
    State(state): State<ServerState>,
) -> Result<Json<u32>, ServerError> {
    let mut conn = state.pool.reader_conn().await?;
    let count = submission::db::count_submissions_completed(&mut conn).await?;
    Ok(Json(count.try_into()?))
}

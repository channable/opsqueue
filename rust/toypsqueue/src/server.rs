use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::http::StatusCode;
use axum::{Json, Router};
use super::submission;
use super::submission::{Submission, Metadata};
use super::chunk::Chunk;

#[derive(Debug, Clone)]
struct ServerState {
    pool: sqlx::SqlitePool,
}

impl ServerState {
    async fn new(database_filename: &str) -> Self {
        let pool = crate::db_connect_pool(database_filename).await;
        ServerState { pool }
    }
}

pub async fn serve(database_filename: &str, server_addr: &str) {
    let state = ServerState::new(database_filename).await;

    let app = Router::new()
    .route("/submissions_count", get(submissions_count))
    .route("/submissions_count_completed", get(submissions_count_completed))
    .route("/insert_submission", post(insert_submission))
    .route("/submission/:submission_id", get(submission_status))
    .with_state(state)
    ;

    let listener = tokio::net::TcpListener::bind(server_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// Make our own error that wraps `anyhow::Error`.
struct ServerError(anyhow::Error);

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            self.0.to_string()
        ).into_response()
    }
}

impl<E> From<E> for ServerError
where
  E: Into<anyhow::Error>
{
fn from(err: E) -> Self {
    Self(err.into())
}
}

async fn submission_status(State(state): State<ServerState>, Path(submission_id): Path<i64>) -> Result<Json<submission::SubmissionStatus>, ServerError> {
    let status = submission::submission_status(submission_id, &state.pool).await?;
    Ok(Json(status))
}

async fn insert_submission(
    State(state): State<ServerState>, 
    Json(request): Json<InsertSubmission>,
) -> Result<Json<InsertSubmissionResponse>, ServerError> {
    let submission_id = Submission::generate_id();
    let iter = (0..request.chunk_count).map(|chunk_index| {
        Chunk::new(submission_id, chunk_index, vec![1,2,3,4])
    });
    let submission = Submission { id: submission_id, chunks_total: request.chunk_count.into(), chunks_done: 0, metadata: request.metadata};

    let mut conn = state.pool.acquire().await?;

    submission::insert_submission(submission, iter, &mut conn).await?;

    Ok(Json(InsertSubmissionResponse{id: submission_id}))
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct InsertSubmission {
    directory_uri: String,
    chunk_count: u32,
    metadata: Option<Metadata>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct InsertSubmissionResponse {
    id: i64,
}

async fn submissions_count(State(state): State<ServerState>) -> Result<Json<u32>, ServerError> {
    let count = submission::count_submissions(&state.pool).await?;
    Ok(Json(count.try_into()?))
}


async fn submissions_count_completed(State(state): State<ServerState>) -> Result<Json<u32>, ServerError> {
    let count = submission::count_submissions_completed(&state.pool).await?;
    Ok(Json(count.try_into()?))
}

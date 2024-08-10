use crate::common::chunk::Chunk;
use crate::common::submission::{self, Metadata, Submission};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

#[derive(Debug, Clone)]
pub struct ServerState {
    pool: sqlx::SqlitePool,
}

impl ServerState {
    async fn new(database_filename: &str) -> Self {
        let pool = crate::db_connect_pool(database_filename).await;
        ServerState { pool }
    }

    pub fn new_from_pool(pool: sqlx::SqlitePool) -> Self {
        ServerState { pool }
    }
    pub async fn serve(self, server_addr: Box<str>) {

        let app = Router::new()
            .route("/submissions_count", get(submissions_count))
            .route(
                "/submissions_count_completed",
                get(submissions_count_completed),
            )
            .route("/insert_submission", post(insert_submission))
            .route("/submission/:submission_id", get(submission_status))
            .with_state(self);

        let listener = tokio::net::TcpListener::bind(&*server_addr).await.unwrap();

        println!("Server running at {server_addr}...");
        axum::serve(listener, app).await.unwrap();
    }
}

pub async fn serve(database_filename: &str, server_addr: Box<str>) {
  ServerState::new(database_filename).await.serve(server_addr).await;
}
// Make our own error that wraps `anyhow::Error`.
struct ServerError(anyhow::Error);

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(self.0.to_string())).into_response()
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
    Path(submission_id): Path<i64>,
) -> Result<Json<submission::SubmissionStatus>, ServerError> {
    let status = submission::submission_status(submission_id, &state.pool).await?;
    Ok(Json(status))
}

async fn insert_submission(
    State(state): State<ServerState>,
    Json(request): Json<InsertSubmission>,
) -> Result<Json<InsertSubmissionResponse>, ServerError> {
    let submission_id = Submission::generate_id();
    let iter = (0..request.chunk_count)
        .map(|chunk_index| Chunk::new(submission_id, chunk_index, vec![1, 2, 3, 4]));
    let submission = Submission {
        id: submission_id,
        chunks_total: request.chunk_count.into(),
        chunks_done: 0,
        metadata: request.metadata,
    };

    let mut conn = state.pool.acquire().await?;

    submission::insert_submission(submission, iter, &mut conn).await?;

    Ok(Json(InsertSubmissionResponse { id: submission_id }))
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct InsertSubmission {
    pub directory_uri: String,
    pub chunk_count: u32,
    pub metadata: Option<Metadata>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct InsertSubmissionResponse {
    pub id: i64,
}

async fn submissions_count(State(state): State<ServerState>) -> Result<Json<u32>, ServerError> {
    let count = submission::count_submissions(&state.pool).await?;
    Ok(Json(count.try_into()?))
}

async fn submissions_count_completed(
    State(state): State<ServerState>,
) -> Result<Json<u32>, ServerError> {
    let count = submission::count_submissions_completed(&state.pool).await?;
    Ok(Json(count.try_into()?))
}

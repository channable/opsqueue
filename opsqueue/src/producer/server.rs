use crate::common::chunk;
use crate::common::submission::{self, Metadata, SubmissionId};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

pub async fn serve_for_tests(database_pool: sqlx::SqlitePool, server_addr: Box<str>) {
    ServerState::new(database_pool)
        .serve_for_tests(server_addr)
        .await;
}


#[derive(Debug, Clone)]
pub struct ServerState {
    pool: sqlx::SqlitePool,
}

impl ServerState {
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        ServerState { pool }
    }
    pub async fn serve_for_tests(self, server_addr: Box<str>) {
        let app = Router::new().nest("/producer", self.build_router()) ;

        let listener = tokio::net::TcpListener::bind(&*server_addr).await.expect("Failed to bind to producer server address");

        tracing::info!("Producer HTTP server listening at {server_addr}...");
        axum::serve(listener, app).await.expect("Failed to start producer server");
    }

    pub fn build_router(self) -> Router<()> {
        Router::new()
            .route("/submissions", post(insert_submission))
            .route(
                // TODO: Probably should get folded into the main 'submissions/count' route (make it return the counts of 'inprogress', 'completed' and 'failed' at the same time)
                "/submissions/count_completed",
                get(submissions_count_completed),
            )
            .route("/submissions/count", get(submissions_count))
            .route("/submissions/:submission_id", get(submission_status))
            .route("/ping", get(ping))
            // TODO: Cancel a submission from the producer side
            .with_state(self)

    }
}


// Make our own error that wraps `anyhow::Error`.
struct ServerError(anyhow::Error);

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
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
    let status = submission::submission_status(submission_id, &state.pool).await?;
    Ok(Json(status))
}

/// Used as a very simple health check by consul
async fn ping(_: State<ServerState>) -> &'static str {
    "pong"
}

async fn insert_submission(
    State(state): State<ServerState>,
    Json(request): Json<InsertSubmission>,
) -> Result<Json<InsertSubmissionResponse>, ServerError> {
    let mut conn = state.pool.acquire().await?;
    let (prefix, chunk_contents) = match request.chunk_contents {
        ChunkContents::Direct{contents} => (None, contents),
        ChunkContents::SeeObjectStorage{prefix, count} => (Some(prefix), (0..count).map(|_index| None).collect()),
    };
    let submission_id =
      submission::insert_submission_from_chunks(
        prefix,
        chunk_contents,
        request.metadata,
        &mut conn)
      .await?;
    Ok(Json(InsertSubmissionResponse { id: submission_id }))
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct InsertSubmission {
    pub chunk_contents: ChunkContents,
    pub metadata: Option<Metadata>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ChunkContents {
    /// Use the `prefix` + the indexes 0..count
    /// to recover the contents of a chunk in the consumer.
    ///
    /// This is what you should use in production.
    SeeObjectStorage{prefix: String, count: i64},
    /// Directly pass the contents of each chunk in Opsqueue itself.
    ///
    /// NOTE: This is useful for small tests/examples,
    /// but significantly less scalable than using `SeeObjectStorage`.
    Direct{contents: Vec<chunk::Content>},
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct InsertSubmissionResponse {
    pub id: SubmissionId,
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

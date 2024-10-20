use crate::common::chunk;
use crate::common::submission::{self, Metadata, SubmissionId};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

pub async fn serve(database_pool: sqlx::SqlitePool, server_addr: Box<str>) {
    ServerState::new(database_pool)
        .serve(server_addr)
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
    pub async fn serve(self, server_addr: Box<str>) {
    let tracing_middleware = 
        tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(tower_http::trace::DefaultMakeSpan::new() )
        .on_request(tower_http::trace::DefaultOnRequest::new()
        )
        .on_response(tower_http::trace::DefaultOnResponse::new()
            .level(tracing::Level::INFO));

        let app = Router::new()
            .route("/submissions", post(insert_submission2))
            .route(
                // TODO: Probably should get folded into the main 'submissions/count' route (make it return the counts of 'inprogress', 'completed' and 'failed' at the same time)
                "/submissions/count_completed",
                get(submissions_count_completed),
            )
            .route("/submissions/count", get(submissions_count))
            .route("/submissions/:submission_id", get(submission_status))
            // TODO: Cancel a submission from the producer side
            .with_state(self)
           .layer(tracing_middleware);

        let listener = tokio::net::TcpListener::bind(&*server_addr).await.expect("Failed to bind to producer server address");

        tracing::info!("Producer HTTP server listening at {server_addr}...");
        axum::serve(listener, app).await.expect("Failed to start producer server");
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

async fn insert_submission(
    State(state): State<ServerState>,
    Json(request): Json<InsertSubmission>,
) -> Result<Json<InsertSubmissionResponse>, ServerError> {
    let mut conn = state.pool.acquire().await?;

    let chunks_contents = (0..request.chunk_count).map(|_index| None).collect();

    let submission_id =
        submission::insert_submission_from_chunks(request.metadata, chunks_contents, &mut conn)
            .await?;
    Ok(Json(InsertSubmissionResponse { id: submission_id }))
}

async fn insert_submission2(
    State(state): State<ServerState>,
    Json(request): Json<InsertSubmission2>,
) -> Result<Json<InsertSubmissionResponse>, ServerError> {
    let mut conn = state.pool.acquire().await?;
    let chunk_contents = match request.chunk_contents {
        ChunkContents::Direct{contents} => contents,
        ChunkContents::SeeObjectStorage{count} => (0..count).map(|_index| None).collect(),
    };
    let submission_id =
      submission::insert_submission_from_chunks(request.metadata, chunk_contents, &mut conn)
      .await?;
    Ok(Json(InsertSubmissionResponse { id: submission_id }))
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct InsertSubmission2 {
    pub prefix: Box<str>,
    pub chunk_contents: ChunkContents,
    pub metadata: Option<Metadata>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ChunkContents {
    /// Use the `prefix` + the indexes 0..count
    /// to recover the contents of a chunk in the consumer.
    /// 
    /// This is what you should use in production.
    SeeObjectStorage{count: u32},
    /// Directly pass the contents of each chunk in Opsqueue itself.
    /// 
    /// NOTE: This is useful for small tests/examples,
    /// but significantly less scalable than using `SeeObjectStorage`.
    Direct{contents: Vec<chunk::Content>},
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct InsertSubmission {
    pub directory_uri: String,
    pub chunk_count: u32,
    pub metadata: Option<Metadata>,
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

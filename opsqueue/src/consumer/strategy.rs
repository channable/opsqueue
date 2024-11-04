use serde::{Deserialize, Serialize};

#[cfg(feature = "server-logic")]
use futures::stream::BoxStream;
#[cfg(feature = "server-logic")]
use futures::Stream;
#[cfg(feature = "server-logic")]
use futures::StreamExt;

#[cfg(feature = "server-logic")]
use sqlx::{SqliteConnection, SqliteExecutor};

#[cfg(feature = "server-logic")]
use crate::common::chunk::{Chunk, ChunkIndex, ChunkCount};
#[cfg(feature = "server-logic")]
use crate::common::submission::{Submission, SubmissionId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    Oldest,
    Newest,
    Random,
    // Custom(CustomStrategy), // TODO
}

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// pub struct CustomStrategy {
//     /// Name, used for debugging.
//     pub name: String,
//     /// Function pointer to the custom strategy's implementation.
//     pub implementation: fn(&mut SqliteConnection) -> ChunkStream<'_>,
// }

#[cfg(feature = "server-logic")]
type ChunkStream<'a> = BoxStream<'a, Result<(Chunk, Submission), sqlx::Error>>;

#[cfg(feature = "server-logic")]
impl Strategy {
    pub fn execute<'c>(&self, db_conn: &'c mut SqliteConnection) -> ChunkStream<'c> {
        match self {
            Strategy::Oldest => oldest_chunks_stream(db_conn).boxed(),
            Strategy::Newest => newest_chunks_stream(db_conn).boxed(),
            Strategy::Random => random_chunks_stream(db_conn).boxed(),
            // Strategy::Custom(CustomStrategy{implementation , ..}) => implementation(db_conn),
        }
    }
}

// #[tracing::instrument]
#[cfg(feature = "server-logic")]
pub fn oldest_chunks_stream<'c>(
    db_conn: impl SqliteExecutor<'c> + 'c,
) -> impl Stream<Item = Result<(Chunk, Submission), sqlx::Error>> + Send + 'c {
    sqlx::query!(r#"
        SELECT
            chunks.submission_id AS "submission_id: SubmissionId"
            , chunks.chunk_index AS "chunk_index: ChunkIndex"
            , chunks.input_content
            , chunks.retries
            , submissions.prefix
            , submissions.chunks_total AS "chunks_total: ChunkCount"
            , submissions.chunks_done AS "chunks_done: ChunkCount"
            , submissions.metadata
         FROM chunks
        INNER JOIN submissions ON submissions.id = chunks.submission_id
        ORDER BY submission_id ASC
    "#
    )
    .fetch(db_conn)
    .map(|res| {
        res.map(|row| {
            (
                Chunk {
                    submission_id: row.submission_id,
                    chunk_index: row.chunk_index,
                    input_content: row.input_content,
                    retries: row.retries,
                },
                Submission {
                    id: row.submission_id,
                    prefix: row.prefix,
                    chunks_total: row.chunks_total,
                    chunks_done: row.chunks_done,
                    metadata: row.metadata,
                },
            )
        })
    })
}

#[tracing::instrument]
#[cfg(feature = "server-logic")]
pub fn newest_chunks_stream<'c>(
    db_conn: impl SqliteExecutor<'c> + 'c,
) -> impl Stream<Item = Result<(Chunk, Submission), sqlx::Error>> + Send + 'c {
    sqlx::query!(r#"
        SELECT
            chunks.submission_id AS "submission_id: SubmissionId"
            , chunks.chunk_index AS "chunk_index: ChunkIndex"
            , chunks.input_content
            , chunks.retries
            , submissions.prefix
            , submissions.chunks_total AS "chunks_total: ChunkCount"
            , submissions.chunks_done AS "chunks_done: ChunkCount"
            , submissions.metadata
         FROM chunks
        INNER JOIN submissions ON submissions.id = chunks.submission_id
        ORDER BY submission_id DESC
    "#
    )
    .fetch(db_conn)
    .map(|res| {
        res.map(|row| {
            (
                Chunk {
                    submission_id: row.submission_id,
                    chunk_index: row.chunk_index,
                    input_content: row.input_content,
                    retries: row.retries,
                },
                Submission {
                    id: row.submission_id,
                    prefix: row.prefix,
                    chunks_total: row.chunks_total,
                    chunks_done: row.chunks_done,
                    metadata: row.metadata,
                },
            )
        })
    })
}


#[tracing::instrument]
#[cfg(feature = "server-logic")]
pub fn random_chunks_stream<'c>(
    db_conn: impl SqliteExecutor<'c> + 'c,
) -> impl Stream<Item = Result<(Chunk, Submission), sqlx::Error>> + Send + 'c {
    sqlx::query!(r#"
        SELECT
            chunks.submission_id AS "submission_id: SubmissionId"
            , chunks.chunk_index AS "chunk_index: ChunkIndex"
            , chunks.input_content
            , chunks.retries
            , submissions.prefix
            , submissions.chunks_total AS "chunks_total: ChunkCount"
            , submissions.chunks_done AS "chunks_done: ChunkCount"
            , submissions.metadata
         FROM chunks
        INNER JOIN submissions ON submissions.id = chunks.submission_id
        ORDER BY random_order
    "#
    )
    .fetch(db_conn)
    .map(|res| {
        res.map(|row| {
            (
                Chunk {
                    submission_id: row.submission_id,
                    chunk_index: row.chunk_index,
                    input_content: row.input_content,
                    retries: row.retries,
                },
                Submission {
                    id: row.submission_id,
                    prefix: row.prefix,
                    chunks_total: row.chunks_total,
                    chunks_done: row.chunks_done,
                    metadata: row.metadata,
                },
            )
        })
    })
}

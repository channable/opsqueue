use std::pin::Pin;

use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::TryStream;
use serde::{Deserialize, Serialize};
use sqlx::{SqliteConnection, SqliteExecutor};

use crate::common::chunk::Chunk;
use crate::common::submission::Submission;

type ChunkStream<'a> =
    Pin<Box<(dyn Stream<Item = Result<(Chunk,Submission), sqlx::Error>> + std::marker::Send + 'a)>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    Oldest,
    Newest,
    // Custom(CustomStrategy), // TODO
}

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// pub struct CustomStrategy {
//     /// Name, used for debugging.
//     pub name: String,
//     /// Function pointer to the custom strategy's implementation.
//     pub implementation: fn(&mut SqliteConnection) -> ChunkStream<'_>,
// }

impl Strategy {
    pub fn execute<'c>(&self, db_conn: &'c mut SqliteConnection) -> ChunkStream<'c> {
        match self {
            Strategy::Oldest => oldest_chunks_stream(db_conn).boxed(),
            Strategy::Newest => newest_chunks_stream(db_conn).boxed(),
            // Strategy::Custom(CustomStrategy{implementation , ..}) => implementation(db_conn),
        }
    }
}

// #[tracing::instrument]
pub fn oldest_chunks_stream<'c>(db_conn: impl SqliteExecutor<'c> + 'c) -> impl Stream<Item = Result<(Chunk, Submission), sqlx::Error>> + Send + 'c {
    sqlx::query!("
    SELECT * FROM chunks 
    INNER JOIN submissions ON submissions.id = chunks.submission_id 
    ORDER BY submission_id ASC
    ").fetch(db_conn).map(|res| res.and_then(|row| {
        Ok((
            Chunk {
                submission_id: row.submission_id.into(),
                chunk_index: row.chunk_index.into(),
                input_content: row.input_content,
                retries: row.retries,
            },
            Submission {
                id: row.submission_id.into(),
                prefix: row.prefix,
                chunks_total: row.chunks_total,
                chunks_done: row.chunks_done,
                metadata: row.metadata,
            }
    ))

    }))
}

#[tracing::instrument]
pub fn newest_chunks_stream<'c>(db_conn: impl SqliteExecutor<'c> + 'c) -> impl Stream<Item = Result<(Chunk, Submission), sqlx::Error>> + Send + 'c {
    sqlx::query!(
        "SELECT * FROM chunks 
        INNER JOIN submissions ON submissions.id = chunks.submission_id 
        ORDER BY submission_id ASC
    ").fetch(db_conn).map(|res| res.and_then(|row| {
            Ok((
                Chunk {
                    submission_id: row.submission_id.into(),
                    chunk_index: row.chunk_index.into(),
                    input_content: row.input_content,
                    retries: row.retries,
                },
                Submission {
                    id: row.submission_id.into(),
                    prefix: row.prefix,
                    chunks_total: row.chunks_total,
                    chunks_done: row.chunks_done,
                    metadata: row.metadata,
                }
        ))
    }))
}

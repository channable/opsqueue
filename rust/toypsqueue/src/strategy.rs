use std::pin::Pin;

use futures::Stream;
use sqlx::{SqliteConnection, SqliteExecutor};

use crate::chunk::Chunk;

type ChunkStream<'a> =
    Pin<Box<(dyn Stream<Item = Result<Chunk, sqlx::Error>> + std::marker::Send + 'a)>>;

#[derive(Debug, Clone)]
pub enum Strategy {
    Oldest,
    Newest,
    Custom(fn(&mut SqliteConnection) -> ChunkStream<'_>),
}

impl Strategy {
    pub fn execute<'c>(&self, db_conn: &'c mut SqliteConnection) -> ChunkStream<'c> {
        match self {
            Strategy::Oldest => oldest_chunks_stream(db_conn),
            Strategy::Newest => newest_chunks_stream(db_conn),
            Strategy::Custom(fun) => fun(db_conn),
        }
    }
}

pub fn oldest_chunks_stream<'c>(db_conn: impl SqliteExecutor<'c> + 'c) -> ChunkStream<'c> {
    sqlx::query_as!(Chunk, "SELECT * FROM chunks ORDER BY submission_id ASC",).fetch(db_conn)
}

pub fn newest_chunks_stream<'c>(db_conn: impl SqliteExecutor<'c> + 'c) -> ChunkStream<'c> {
    sqlx::query_as!(Chunk, "SELECT * FROM chunks ORDER BY submission_id ASC",).fetch(db_conn)
}

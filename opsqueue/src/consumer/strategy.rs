use serde::{Deserialize, Serialize};

#[cfg(feature = "server-logic")]
use futures::stream::BoxStream;

#[cfg(feature = "server-logic")]
use sqlx::{SqliteConnection, SqliteExecutor};

use crate::common::chunk::Chunk;
#[cfg(feature = "server-logic")]
use crate::common::chunk::ChunkId;
// #[cfg(feature = "server-logic")]
// use crate::common::chunk::ChunkIndex;
// #[cfg(feature = "server-logic")]
// use crate::common::submission::SubmissionId;

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
type ChunkIdStream<'a> = BoxStream<'a, Result<ChunkId, sqlx::Error>>;

#[cfg(feature = "server-logic")]
pub type ChunkStream<'a> = BoxStream<'a, Result<Chunk, sqlx::Error>>;

#[cfg(feature = "server-logic")]
impl Strategy {
    pub fn execute<'c>(&self, db_conn: &'c mut SqliteConnection) -> ChunkStream<'c> {
        match self {
            Strategy::Oldest => oldest_chunks_stream(db_conn),
            Strategy::Newest => newest_chunks_stream(db_conn),
            Strategy::Random => random_chunks_stream(db_conn),
            // Strategy::Custom(CustomStrategy{implementation , ..}) => implementation(db_conn),
        }
    }
}

// #[tracing::instrument]
#[cfg(feature = "server-logic")]
pub fn oldest_chunks_stream<'c>(
    db_conn: impl SqliteExecutor<'c> + 'c,
) -> ChunkStream<'c> {
    sqlx::query_as!(Chunk, r#"
        SELECT
            chunks.submission_id AS "submission_id: _"
            , chunks.chunk_index AS "chunk_index: _"
            , input_content AS "input_content: _"
            , retries
         FROM chunks
        ORDER BY submission_id ASC
    "#
    )
    .fetch(db_conn)
}

#[tracing::instrument]
#[cfg(feature = "server-logic")]
pub fn newest_chunks_stream<'c>(
    db_conn: impl SqliteExecutor<'c> + 'c,
) -> ChunkStream<'c> {
    sqlx::query_as!(Chunk, r#"
        SELECT
            chunks.submission_id AS "submission_id: _"
            , chunks.chunk_index AS "chunk_index: _"
            , input_content AS "input_content: _"
            , retries
        FROM chunks
        ORDER BY submission_id DESC
    "#
    )
    .fetch(db_conn)
}

/// Select one or more chunks in a random order.
///
/// We use two tricks to ensure this is both efficient and truly random:
/// 1. The random order is determined _on insert_, c.f. the VIRTUAL `random_order` column and associated index.
///    It ensures that chunks (especially consecutive chunks of the same submission)
///    are spread out evenly across the 16-bit range.
///    You can view this as 'shuffling the deck'.
/// 2. On _read_, pick a random permutation of this order.
///    This is implemented by picking a random offset,
///    reading everything after that, and then everything before that.
///    You can view this as 'cutting the deck'.
///
/// The goal of (1) is providing fairness with an efficient query.
/// The main goal of (2) is to reduce lock contention on the reserver, further improving performance.
///
/// A secondary goal of (2) is to make the random order non-deterministic,
/// which is mainly important when chunks need to be retried
/// (they should be retried 'whenever' rather than 'be placed back on the top of the deck')
///
/// Note that (2) without (1) would not be sufficiently random/fair.
/// (2) only works because data is spread out uniformly, which is not the case for the normal
/// `ChunkId`.
#[tracing::instrument]
#[cfg(feature = "server-logic")]
pub fn random_chunks_stream<'c>(
    db_conn: impl SqliteExecutor<'c> + 'c,
) -> ChunkStream<'c> {
    let random_offset: u16 = rand::random();
    // NOTE 1: Until https://github.com/launchbadge/sqlx/issues/1151
    // is fixed, we'll have to use the runtime `query_as`
    // rather than the compile-time checked one here.

    // NOTE 2: In theory the UNION ALL could be optimized and we lose the randomness.
    // The current version of SQLite doesn't do this, instead simply running the first and then the second.
    //
    // If it ever would become a problem, we can instead run two consecutive queries.
    let query =
        r#"
        SELECT
            submission_id
            , chunk_index
            , input_content
            , retries
        FROM chunks
        WHERE chunks.random_order >= ?
        UNION ALL
        SELECT
            submission_id
            , chunk_index
            , input_content
            , retries
        FROM chunks
        WHERE chunks.random_order < ?
    "#;
    sqlx::query_as(query).bind(random_offset).bind(random_offset)
    .fetch(db_conn)

    // Alternatively, we could do this 100% in SQL:
    /*

    sqlx::query_as!(ChunkId, r#"
    WITH const AS (select RANDOM() % 65536 AS random_offset)
    SELECT
          chunks.submission_id AS "submission_id: _"
        , chunks.chunk_index AS "chunk_index: _"
    FROM chunks, const
    WHERE chunks.random_order >= const.random_offset
    UNION ALL
    SELECT
          chunks.submission_id AS "submission_id: _"
        , chunks.chunk_index AS "chunk_index: _"
    FROM chunks, const
    WHERE chunks.random_order < const.random_offset;
    "#).fetch(db_conn)

    */
}

#[cfg(feature = "server-logic")]
use std::ops::{Deref, DerefMut};

use chrono::NaiveDateTime;

use serde::{Deserialize, Serialize};
#[cfg(feature = "server-logic")]
use sqlx::{query, Connection, Executor, QueryBuilder, Sqlite, SqliteExecutor};
#[cfg(feature = "server-logic")]
use sqlx::{query_as, SqliteConnection};
use ux_serde::u63;

use super::errors::{ChunkNotFound, DatabaseError, Either, SubmissionNotFound};
use super::submission::SubmissionId;
use super::MayBeZero;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ChunkIndex(i64);

#[derive(thiserror::Error, Debug)]
#[error("{0} is not a valid chunk index")]
pub struct InvalidChunkIndexError(pub i64);

impl ChunkIndex {
    pub fn new(index: i64) -> Result<Self, InvalidChunkIndexError> {
        if index < 0 {
            Err(InvalidChunkIndexError(index))
        } else {
            Ok(ChunkIndex(index))
        }
    }
}

impl MayBeZero for ChunkIndex {
    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl std::fmt::Display for ChunkIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(feature = "server-logic")]
impl<'q> sqlx::Encode<'q, Sqlite> for ChunkIndex {
    fn encode(
        self,
        buf: &mut <Sqlite as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull
    where
        Self: Sized,
    {
        <i64 as sqlx::Encode<'q, Sqlite>>::encode(self.0, buf)
    }
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        <i64 as sqlx::Encode<'q, Sqlite>>::encode_by_ref(&self.0, buf)
    }
}

/// NOTE: Only exists to please SQLx! Do not use directly!
///
/// Use the u63 conversion instead.
impl From<i64> for ChunkIndex {
    fn from(value: i64) -> Self {
        ChunkIndex(value)
    }
}

impl From<ChunkIndex> for u63 {
    fn from(value: ChunkIndex) -> Self {
        u63::new(value.0 as u64)
    }
}

impl From<u63> for ChunkIndex {
    fn from(value: u63) -> Self {
        ChunkIndex(u64::from(value) as i64)
    }
}

#[cfg(feature = "server-logic")]
impl sqlx::Type<Sqlite> for ChunkIndex {
    fn compatible(ty: &<Sqlite as sqlx::Database>::TypeInfo) -> bool {
        <i64 as sqlx::Type<Sqlite>>::compatible(ty)
    }
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <i64 as sqlx::Type<Sqlite>>::type_info()
    }
}

pub type ChunkId = (SubmissionId, ChunkIndex);

pub type Content = Option<Vec<u8>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    pub submission_id: SubmissionId,
    pub chunk_index: ChunkIndex,
    pub input_content: Content,
    pub retries: i64,
}

pub struct ChunkCompleted {
    pub submission_id: SubmissionId,
    pub chunk_index: ChunkIndex,
    pub output_content: Content,
    pub completed_at: NaiveDateTime,
}

impl Chunk {
    pub fn new(submission_id: SubmissionId, chunk_index: ChunkIndex, uri: Content) -> Self {
        Chunk {
            submission_id,
            chunk_index,
            input_content: uri,
            retries: 0,
        }
    }
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn insert_chunk(
    chunk: Chunk,
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<()> {
    query!(
        "INSERT INTO chunks (submission_id, chunk_index, input_content) VALUES (?, ?, ?)",
        chunk.submission_id,
        chunk.chunk_index,
        chunk.input_content
    )
    .execute(conn)
    .await?;
    Ok(())
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn complete_chunk(
    full_chunk_id: ChunkId,
    output_content: Option<Vec<u8>>,
    conn: &mut SqliteConnection,
) -> Result<(), Either<DatabaseError, Either<SubmissionNotFound, ChunkNotFound>>> {
    conn.transaction(|tx| {
        Box::pin(async move {
            complete_chunk_raw(full_chunk_id, output_content, &mut **tx).await?;
            super::submission::maybe_complete_submission(full_chunk_id.0, tx)
                .await
                .map_err(|e| match e {
                    Either::Left(e) => Either::Left(e),
                    Either::Right(e) => Either::Right(Either::Left(e)),
                })?;
            Ok(())
        })
    })
    .await
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
/// TODO: Complete submission automatically when all chunks are completed
pub async fn complete_chunk_raw(
    full_chunk_id: ChunkId,
    output_content: Option<Vec<u8>>,
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<()> {
    let now = chrono::prelude::Utc::now();
    query!("
    SAVEPOINT complete_chunk_raw;

    UPDATE submissions SET chunks_done = chunks_done + 1 WHERE submissions.id = ?;

    INSERT INTO chunks_completed
    (submission_id, chunk_index, output_content, completed_at)
    SELECT submission_id, chunk_index, ?, julianday(?) FROM chunks WHERE chunks.submission_id = ? AND chunks.chunk_index = ?;

    DELETE FROM chunks WHERE chunks.submission_id = ? AND chunks.chunk_index = ?;


    RELEASE SAVEPOINT complete_chunk_raw;
    ",
    full_chunk_id.0,
    output_content,
    now,
    full_chunk_id.0,
    full_chunk_id.1,
    full_chunk_id.0,
    full_chunk_id.1,
    ).execute(conn).await?;
    Ok(())
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn retry_or_fail_chunk(
    full_chunk_id: ChunkId,
    failure: String,
    conn: &mut SqliteConnection,
) -> sqlx::Result<()> {
    conn.transaction(|tx| {
        Box::pin(async move {
            const MAX_RETRIES: i64 = 10;
            let (submission_id, chunk_index) = full_chunk_id;
            let fields = query!(
                "
        UPDATE chunks SET retries = retries + 1
        WHERE submission_id = ? AND chunk_index = ?
        RETURNING retries
        ;
        ",
                submission_id,
                chunk_index
            )
            .fetch_one(&mut **tx)
            .await?;
            if fields.retries >= MAX_RETRIES {
                super::submission::fail_submission(submission_id, chunk_index, failure, tx).await?
            }
            Ok(())
        })
    })
    .await
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn move_chunk_to_failed_chunks(
    full_chunk_id: ChunkId,
    failure: String,
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<()> {
    let now = chrono::prelude::Utc::now();
    let (submission_id, chunk_index) = full_chunk_id;
    query!("
    SAVEPOINT move_chunk_to_failed_chunks;

    INSERT INTO chunks_failed
    (submission_id, chunk_index, input_content, failure, failed_at)
    SELECT submission_id, chunk_index, input_content, ?, julianday(?) FROM chunks WHERE chunks.submission_id = ? AND chunks.chunk_index = ?;

    DELETE FROM chunks WHERE chunks.submission_id = ? AND chunks.chunk_index = ? RETURNING *;

    RELEASE SAVEPOINT move_chunk_to_failed_chunks;
    ",
    failure,
    now,
    submission_id,
    chunk_index,
    submission_id,
    chunk_index,
    ).fetch_one(conn).await?;
    Ok(())
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn get_chunk(
    full_chunk_id: ChunkId,
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<Chunk> {
    query_as!(
        Chunk,
        "SELECT * FROM chunks WHERE submission_id =? AND chunk_index =?",
        full_chunk_id.0,
        full_chunk_id.1
    )
    .fetch_one(conn)
    .await
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn get_chunk_completed(
    full_chunk_id: (i64, i64),
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<ChunkCompleted> {
    query_as!(ChunkCompleted, "SELECT submission_id, chunk_index, output_content, completed_at FROM chunks_completed WHERE submission_id = ? AND chunk_index = ?", full_chunk_id.0, full_chunk_id.1).fetch_one(conn).await
}

#[cfg(feature = "server-logic")]
#[tracing::instrument(skip(chunks, conn))]
pub async fn insert_many_chunks<Tx, Conn, Iter>(chunks: Iter, mut conn: Tx) -> sqlx::Result<()>
where
    for<'a> &'a mut Conn: Executor<'a, Database = Sqlite>,
    Tx: Deref<Target = Conn> + DerefMut,
    Iter: IntoIterator<Item = Chunk> + Send + Sync + 'static,
    <Iter as IntoIterator>::IntoIter: Send + Sync + 'static,
{
    let chunks_per_query = 1000;

    // let start = std::time::Instant::now();
    let mut iter = chunks.into_iter().peekable();
    while iter.peek().is_some() {
        let query_chunks = iter.by_ref().take(chunks_per_query);

        let mut query_builder: QueryBuilder<Sqlite> =
            QueryBuilder::new("INSERT INTO chunks (submission_id, chunk_index, input_content) ");
        query_builder.push_values(query_chunks, |mut b, chunk| {
            b.push_bind(chunk.submission_id)
                .push_bind(chunk.chunk_index)
                .push_bind(chunk.input_content.clone());
        });
        let query = query_builder.build();

        query.execute(conn.deref_mut()).await?;
    }

    Ok(())
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn skip_remaining_chunks(
    submission_id: SubmissionId,
    conn: impl SqliteExecutor<'_>,
) -> sqlx::Result<()> {
    let now = chrono::prelude::Utc::now();

    sqlx::query!("
    SAVEPOINT skip_remaining_chunks;

    INSERT INTO chunks_failed
    (submission_id, chunk_index, input_content, failure, skipped, failed_at)
    SELECT submission_id, chunk_index, input_content, '', 1, julianday(?) FROM chunks WHERE chunks.submission_id = ?;

    DELETE FROM chunks WHERE chunks.submission_id = ?;

    RELEASE SAVEPOINT skip_remaining_chunks;
    ",
    now,
    submission_id,
    submission_id,
    ).execute(conn).await?;
    Ok(())
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn select_random_chunks(db: impl sqlx::SqliteExecutor<'_>, count: u32) -> Vec<Chunk> {
    // TODO: Document what we're doing here exactly
    let count_div10 = std::cmp::max(count / 10, 100);
    sqlx::query_as!(
        Chunk,
        "SELECT submission_id, chunk_index, input_content, retries FROM chunks JOIN
    (SELECT rowid as rid FROM chunks
        WHERE random() % $1 = 0  -- Reduce rowids by Nx
        LIMIT $2) AS srid
    ON chunks.rowid = srid.rid;",
        count_div10,
        count
    )
    .fetch_all(db)
    .await
    .unwrap()
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn count_chunks(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn count_chunks_completed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks_completed;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn count_chunks_failed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks_failed;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {
    use crate::common::submission::{insert_submission_raw, Submission, SubmissionStatus};

    use super::*;

    #[sqlx::test]
    pub async fn test_insert_chunk(db: sqlx::SqlitePool) {
        let chunk = Chunk::new(1.into(), 0.into(), vec![1, 2, 3, 4, 5].into());

        assert!(count_chunks(&db).await.unwrap() == 0);
        insert_chunk(chunk.clone(), &db)
            .await
            .expect("Insert chunk failed");
        assert!(count_chunks(&db).await.unwrap() == 1);
    }

    #[sqlx::test]
    pub async fn test_get_chunk(db: sqlx::SqlitePool) {
        let chunk = Chunk::new(1.into(), 0.into(), vec![1, 2, 3, 4, 5].into());
        insert_chunk(chunk.clone(), &db)
            .await
            .expect("Insert chunk failed");

        let fetched_chunk = get_chunk((chunk.submission_id, chunk.chunk_index), &db)
            .await
            .unwrap();
        assert!(chunk == fetched_chunk);
    }

    #[sqlx::test]
    pub async fn test_complete_chunk_raw(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();

        let mut submission = Submission::new();
        submission.chunks_total = 1;
        submission.id = Submission::generate_id();
        let chunk = Chunk::new(submission.id, 0.into(), vec![1, 2, 3, 4, 5].into());

        insert_chunk(chunk.clone(), &mut *conn)
            .await
            .expect("Insert chunk failed");

        insert_submission_raw(submission, &mut *conn).await.unwrap();

        complete_chunk_raw(
            (chunk.submission_id, chunk.chunk_index),
            Some(vec![6, 7, 8, 9]),
            &mut *conn,
        )
        .await
        .expect("complete chunk failed");

        assert!(count_chunks(&mut *conn).await.unwrap() == 0);
        assert!(count_chunks_completed(&mut *conn).await.unwrap() == 1);
        assert!(count_chunks_failed(&mut *conn).await.unwrap() == 0);
    }

    #[sqlx::test]
    pub async fn test_complete_chunk_raw_updates_submissions_chunk_total(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let submission_id = crate::common::submission::insert_submission_from_chunks(
            None,
            vec![Some("first".into())],
            None,
            &mut conn,
        )
        .await
        .unwrap();

        assert!(count_chunks(&mut *conn).await.unwrap() == 1);

        complete_chunk_raw(
            (submission_id, 0.into()),
            Some(vec![6, 7, 8, 9]),
            &mut *conn,
        )
        .await
        .expect("complete chunk failed");

        let submission_status =
            crate::common::submission::submission_status(submission_id, &mut *conn)
                .await
                .unwrap()
                .unwrap();
        match submission_status {
            SubmissionStatus::InProgress(submission) => {
                assert_eq!(submission.chunks_done, 1);
            }
            _ => panic!("Expected InProgress"),
        }
    }

    #[sqlx::test]
    pub async fn test_fail_chunk(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let chunk = Chunk::new(1.into(), 0.into(), vec![1, 2, 3, 4, 5].into());

        insert_chunk(chunk.clone(), &mut *conn)
            .await
            .expect("Insert chunk failed");
        move_chunk_to_failed_chunks(
            (chunk.submission_id, chunk.chunk_index),
            "Boom!".to_string(),
            &mut *conn,
        )
        .await
        .expect("Succeed chunk failed");

        assert!(count_chunks(&mut *conn).await.unwrap() == 0);
        assert!(count_chunks_completed(&mut *conn).await.unwrap() == 0);
        assert!(count_chunks_failed(&mut *conn).await.unwrap() == 1);
    }
}

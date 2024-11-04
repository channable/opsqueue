
use chrono::{DateTime, Utc};

use serde::{Deserialize, Serialize};
use ux_serde::u63;


use super::errors::TryFromIntError;
use super::submission::SubmissionId;
use super::MayBeZero;

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]

/// Index of this particular chunk in a submission.
pub struct ChunkIndex(u63);

impl std::fmt::Debug for ChunkIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ChunkIndex")
            .field(&self.0)
            .finish()
    }
}

/// Another name for ChunkIndex, indicating that we're dealing with the _total count_ of chunks.
/// i.e. when you have a value of type ChunkCount, there is a high likelyhood
/// that there are [0..chunk_count) (note the half-open range) chunks to select from.
pub type ChunkCount = ChunkIndex;

// #[derive(thiserror::Error, Debug)]
// #[error("{0} is not a valid chunk index")]
// pub struct InvalidChunkIndexError(pub i64);

impl ChunkIndex {
    pub fn new<T>(index: T) -> Result<Self, TryFromIntError>
    where
        Self: TryFrom<T, Error = TryFromIntError>,
    {
        Self::try_from(index)
    }
    pub fn zero() -> Self {
        Self(u63::new(0))
    }
}

impl MayBeZero for ChunkIndex {
    fn is_zero(&self) -> bool {
        self.0 == u63::new(0)
    }
}

impl std::fmt::Display for ChunkIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}


impl From<ChunkIndex> for u63 {
    fn from(value: ChunkIndex) -> Self {
        value.0
    }
}

impl From<u63> for ChunkIndex {
    fn from(value: u63) -> Self {
        ChunkIndex(value)
    }
}


impl From<ChunkIndex> for u64 {
    fn from(value: ChunkIndex) -> Self {
        value.0.into()
    }
}

impl From<ChunkIndex> for i64 {
    fn from(value: ChunkIndex) -> Self {
        let inner: u64 = value.0.into();
        // Guaranteed to fit positive signed range
        inner as i64
    }
}

impl TryFrom<u64> for ChunkIndex {
    type Error = crate::common::errors::TryFromIntError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value > u63::MAX.into() {
            return Err(crate::common::errors::TryFromIntError(()));
        }
        Ok(Self(u63::new(value)))
    }
}

impl TryFrom<usize> for ChunkIndex {
    type Error = crate::common::errors::TryFromIntError;
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Self::try_from(value as u64)
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
    pub completed_at: DateTime<Utc>,
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
pub mod db {
    use super::*;
use std::ops::{Deref, DerefMut};
use sqlx::{query, Executor, QueryBuilder, Sqlite, SqliteExecutor};
use sqlx::{query_as, SqliteConnection};
use crate::common::errors::{ChunkNotFound, DatabaseError, E, SubmissionNotFound};

use crate::db::SqliteConnectionExt;


impl<'q> sqlx::Encode<'q, Sqlite> for super::ChunkIndex {
    fn encode_by_ref(
            &self,
            buf: &mut <Sqlite as sqlx::Database>::ArgumentBuffer<'q>,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <i64 as sqlx::Encode<'q, Sqlite>>::encode_by_ref(&i64::from(*self), buf)
    }

    fn encode(self, buf: &mut <Sqlite as sqlx::Database>::ArgumentBuffer<'q>) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError>
        where
            Self: Sized, {
        <i64 as sqlx::Encode<'q, Sqlite>>::encode(i64::from(self), buf)
    }
}

impl sqlx::Type<Sqlite> for ChunkIndex {
    fn compatible(ty: &<Sqlite as sqlx::Database>::TypeInfo) -> bool {
        <u64 as sqlx::Type<Sqlite>>::compatible(ty)
    }
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <u64 as sqlx::Type<Sqlite>>::type_info()
    }
}

impl<'q> sqlx::Decode<'q, Sqlite> for ChunkIndex {
    fn decode(value: <Sqlite as sqlx::Database>::ValueRef<'q>) -> Result<Self, sqlx::error::BoxDynError> {
        let inner = <u64 as sqlx::Decode<'q, Sqlite>>::decode(value)?;
        let x = ChunkIndex::try_from(inner)?;
        Ok(x)
    }
}


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

#[tracing::instrument]
pub async fn complete_chunk(
    full_chunk_id: ChunkId,
    output_content: Option<Vec<u8>>,
    conn: &mut SqliteConnection,
) -> Result<(), E<DatabaseError, E<SubmissionNotFound, ChunkNotFound>>> {

    conn.immediate_write_transaction(|tx| {
        Box::pin(async move {
            complete_chunk_raw(full_chunk_id, output_content, tx).await?;
            crate::common::submission::db::maybe_complete_submission(full_chunk_id.0, tx)
                .await
                .map_err(|e| match e {
                    E::L(e) => E::L(e),
                    E::R(e) => E::R(E::L(e)),
                })?;
            Ok(())
        })
    })
    .await
}

#[tracing::instrument]
/// Do not call directly! MUST be called inside a transaction!
pub async fn complete_chunk_raw(
    full_chunk_id: ChunkId,
    output_content: Option<Vec<u8>>,
    conn: &mut SqliteConnection,
) -> sqlx::Result<()> {
        let now = chrono::prelude::Utc::now();
        query!("

        INSERT INTO chunks_completed
        (submission_id, chunk_index, output_content, completed_at)
        SELECT submission_id, chunk_index, ?, julianday(?) FROM chunks
        WHERE chunks.submission_id = ? AND chunks.chunk_index = ?;

        DELETE FROM chunks WHERE chunks.submission_id = ? AND chunks.chunk_index = ?
        RETURNING submission_id, chunk_index;
        ;
        ",
        output_content,
        now,
        full_chunk_id.0,
        full_chunk_id.1,
        full_chunk_id.0,
        full_chunk_id.1,
        ).fetch_one(&mut *conn).await?;
        // Defense in depth: Above query should never be called twice on the same chunk.
        // If it _does_ happen, it means that either a consumer is attempting a chunk they didn't reserve,
        // or we gave out the same reservation twice.
        //
        // By returning early if the chunk was not found,
        // we ensure that even in these situations
        // we never mess up the submission's `chunks_done` counter.
        //
        // (Not doing that resulted in a hard-to-track-down bug in the past.
        // https://github.com/channable/opsqueue/issues/76
        // )
        query!("
        UPDATE submissions SET chunks_done = chunks_done + 1 WHERE submissions.id = ?;
        ",
        full_chunk_id.0,
        ).execute(&mut *conn).await?;
    Ok(())
}

#[tracing::instrument]
pub async fn retry_or_fail_chunk(
    full_chunk_id: ChunkId,
    failure: String,
    conn: &mut SqliteConnection,
) -> sqlx::Result<()> {
    conn.immediate_write_transaction(|tx| {
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
                crate::common::submission::db::fail_submission_notx(submission_id, chunk_index, failure, tx).await?
            }
            Ok(())
        })
    })
    .await
}

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

#[tracing::instrument]
pub async fn get_chunk(
    full_chunk_id: ChunkId,
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<Chunk> {
    query_as!(
        Chunk, r#"
        SELECT
            submission_id AS "submission_id: SubmissionId"
            , chunk_index AS "chunk_index: ChunkIndex"
            , input_content
            , retries
        FROM chunks WHERE submission_id =? AND chunk_index =?
        "#,
        full_chunk_id.0,
        full_chunk_id.1
    )
    .fetch_one(conn)
    .await
}

#[tracing::instrument]
pub async fn get_chunk_completed(
    full_chunk_id: (i64, i64),
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<ChunkCompleted> {
    query_as!(ChunkCompleted, r#"
        SELECT
            submission_id AS "submission_id: SubmissionId"
            , chunk_index AS "chunk_index: ChunkIndex"
            , output_content
            , completed_at AS "completed_at: DateTime<Utc>"
        FROM chunks_completed WHERE submission_id = ? AND chunk_index = ?
        "#,
        full_chunk_id.0,
        full_chunk_id.1
    ).fetch_one(conn).await
}

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

// #[tracing::instrument]
// pub async fn select_random_chunks(db: impl sqlx::SqliteExecutor<'_>, count: u32) -> Vec<Chunk> {
//     // TODO: Document what we're doing here exactly
//     let count_div10 = std::cmp::max(count / 10, 100);
//     sqlx::query_as!(
//         Chunk,
//         "SELECT submission_id, chunk_index, input_content, retries FROM chunks JOIN
//     (SELECT rowid as rid FROM chunks
//         WHERE random() % $1 = 0  -- Reduce rowids by Nx
//         LIMIT $2) AS srid
//     ON chunks.rowid = srid.rid;",
//         count_div10,
//         count
//     )
//     .fetch_all(db)
//     .await
//     .unwrap()
// }

#[tracing::instrument]
pub async fn count_chunks(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<u63> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks;")
        .fetch_one(db)
        .await?;
    let count = u63::new(count.count as u64);
    Ok(count)
}

#[tracing::instrument]
pub async fn count_chunks_completed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<u63> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks_completed;")
        .fetch_one(db)
        .await?;
    let count = u63::new(count.count as u64);
    Ok(count)
}

#[tracing::instrument]
pub async fn count_chunks_failed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<u63> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks_failed;")
        .fetch_one(db)
        .await?;
    let count = u63::new(count.count as u64);
    Ok(count)
}
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {
    use crate::common::submission::{Submission, SubmissionStatus};
    use crate::common::submission::db::insert_submission_raw;
    use crate::db::SqliteConnectionExt;

    use super::*;
    use super::db::*;

    #[sqlx::test]
    pub async fn test_insert_chunk(db: sqlx::SqlitePool) {
        let chunk = Chunk::new(u63::new(1).into(), u63::new(0).into(), vec![1, 2, 3, 4, 5].into());

        assert!(count_chunks(&db).await.unwrap() == u63::new(0));
        insert_chunk(chunk.clone(), &db)
            .await
            .expect("Insert chunk failed");
        assert!(count_chunks(&db).await.unwrap() == u63::new(1));
    }

    #[sqlx::test]
    pub async fn test_get_chunk(db: sqlx::SqlitePool) {
        let chunk = Chunk::new(u63::new(1).into(), u63::new(0).into(), vec![1, 2, 3, 4, 5].into());
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
        submission.chunks_total = u63::new(1).into();
        submission.id = Submission::generate_id();
        let chunk = Chunk::new(submission.id, u63::new(0).into(), vec![1, 2, 3, 4, 5].into());

        insert_chunk(chunk.clone(), &mut *conn)
            .await
            .expect("Insert chunk failed");

        insert_submission_raw(submission, &mut *conn).await.unwrap();

        conn.immediate_write_transaction(|tx| Box::pin(async move {
            complete_chunk_raw(
                (chunk.submission_id, chunk.chunk_index),
                Some(vec![6, 7, 8, 9]),
                tx,
            ).await
        })).await.expect("complete chunk failed");

        assert!(count_chunks(&mut *conn).await.unwrap() == u63::new(0));
        assert!(count_chunks_completed(&mut *conn).await.unwrap() == u63::new(1));
        assert!(count_chunks_failed(&mut *conn).await.unwrap() == u63::new(0));
    }

    #[sqlx::test]
    pub async fn test_complete_chunk_raw_updates_submissions_chunk_total(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let submission_id = crate::common::submission::db::insert_submission_from_chunks(
            None,
            vec![Some("first".into())],
            None,
            &mut conn,
        )
        .await
        .unwrap();

        assert!(count_chunks(&mut *conn).await.unwrap() == u63::new(1));

        complete_chunk_raw(
            (submission_id, u63::new(0).into()),
            Some(vec![6, 7, 8, 9]),
            &mut conn,
        )
        .await
        .expect("complete chunk failed");

        let submission_status =
            crate::common::submission::db::submission_status(submission_id, &mut conn)
                .await
                .unwrap()
                .unwrap();
        match submission_status {
            SubmissionStatus::InProgress(submission) => {
                assert_eq!(submission.chunks_done, u63::new(1).into());
            }
            _ => panic!("Expected InProgress"),
        }
    }

    #[sqlx::test]
    pub async fn test_fail_chunk(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let chunk = Chunk::new(u63::new(1).into(), u63::new(0).into(), vec![1, 2, 3, 4, 5].into());

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

        assert!(count_chunks(&mut *conn).await.unwrap() == u63::new(0));
        assert!(count_chunks_completed(&mut *conn).await.unwrap() == u63::new(0));
        assert!(count_chunks_failed(&mut *conn).await.unwrap() == u63::new(1));
    }
}

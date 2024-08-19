use std::ops::{Deref, DerefMut};

use chrono::NaiveDateTime;

use serde::{Deserialize, Serialize};
use sqlx::{query_as, SqliteConnection};
use sqlx::{query, Executor, QueryBuilder, Sqlite, SqliteExecutor};

use super::submission::SubmissionId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct ChunkIndex(i64);

impl std::fmt::Display for ChunkIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'q> sqlx::Encode<'q, Sqlite> for ChunkIndex {
    fn encode(self, buf: &mut <Sqlite as sqlx::database::HasArguments<'q>>::ArgumentBuffer) -> sqlx::encode::IsNull
        where
            Self: Sized, {
                <i64 as sqlx::Encode<'q, Sqlite>>::encode(self.0, buf)
    }
    fn encode_by_ref(&self, buf: &mut <Sqlite as sqlx::database::HasArguments<'q>>::ArgumentBuffer) -> sqlx::encode::IsNull {
                <i64 as sqlx::Encode<'q, Sqlite>>::encode_by_ref(&self.0, buf)
    }
}

impl From<i64> for ChunkIndex {
    fn from(value: i64) -> Self {
        ChunkIndex(value)
    }
}

impl From<ChunkIndex> for i64 {
    fn from(value: ChunkIndex) -> Self {
        value.0
    }
}

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

pub async fn complete_chunk(
    full_chunk_id: ChunkId,
    output_content: Vec<u8>,
    conn: &mut SqliteConnection,
) -> sqlx::Result<()> {
    query!("SAVEPOINT complete_chunk;").execute(&mut *conn).await?;

    complete_chunk_raw(full_chunk_id, output_content, &mut *conn).await?;
    super::submission::maybe_complete_submission(full_chunk_id.0, &mut *conn).await?;
    query!("RELEASE SAVEPOINT complete_chunk;").execute(&mut *conn).await?;

    Ok(())
}

/// TODO: Complete submission automatically when all chunks are completed
pub async fn complete_chunk_raw(
    full_chunk_id: ChunkId,
    output_content: Vec<u8>,
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

pub async fn fail_chunk(
    full_chunk_id: ChunkId,
    failure: Vec<u8>,
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<()> {
    let now = chrono::prelude::Utc::now();
    let (submission_id, chunk_index) = full_chunk_id;
    query!("
    SAVEPOINT fail_chunk;

    INSERT INTO chunks_failed
    (submission_id, chunk_index, input_content, failure, failed_at) 
    SELECT submission_id, chunk_index, input_content, ?, julianday(?) FROM chunks WHERE chunks.submission_id = ? AND chunks.chunk_index = ?;

    DELETE FROM chunks WHERE chunks.submission_id = ? AND chunks.chunk_index = ? RETURNING *;

    RELEASE SAVEPOINT fail_chunk;
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

pub async fn get_chunk_completed(
    full_chunk_id: (i64, i64),
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<ChunkCompleted> {
    query_as!(ChunkCompleted, "SELECT submission_id, chunk_index, output_content, completed_at FROM chunks_completed WHERE submission_id = ? AND chunk_index = ?", full_chunk_id.0, full_chunk_id.1).fetch_one(conn).await
}

pub async fn insert_many_chunks<Tx, Conn>(
    chunks: impl IntoIterator<Item = Chunk>,
    mut conn: Tx,
) -> sqlx::Result<()>
where
    for<'a> &'a mut Conn: Executor<'a, Database = Sqlite>,
    Tx: Deref<Target = Conn> + DerefMut,
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

pub async fn skip_remaining_chunks(
    submission_id: SubmissionId,
    conn: impl SqliteExecutor<'_>,
) -> sqlx::Result<()> {
    let now = chrono::prelude::Utc::now();

    sqlx::query!("
    SAVEPOINT skip_remaining_chunks;

    INSERT INTO chunks_failed
    (submission_id, chunk_index, input_content, failure, failed_at)
    SELECT submission_id, chunk_index, input_content, 'skip', julianday(?) FROM chunks WHERE submission_id = ?;

    RELEASE SAVEPOINT skip_remaining_chunks;
    ",
    submission_id,
    now,
    ).execute(conn).await?;
    Ok(())
}

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

pub async fn count_chunks(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

pub async fn count_chunks_completed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks_completed;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

pub async fn count_chunks_failed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks_failed;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

#[cfg(test)]
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
            vec![6, 7, 8, 9],
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
        let submission_id = crate::common::submission::insert_submission_from_chunks(None, vec![Some("first".into())], &mut *conn).await.unwrap();

        assert!(count_chunks(&mut *conn).await.unwrap() == 1);

        complete_chunk_raw(
            (submission_id, 0.into()),
            vec![6, 7, 8, 9],
            &mut *conn,
        )
        .await
        .expect("complete chunk failed");

        let submission_status = crate::common::submission::submission_status(submission_id, &mut *conn).await.unwrap().unwrap();
        match dbg!(submission_status) {
            SubmissionStatus::InProgress(submission) =>  {
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
        fail_chunk(
            (chunk.submission_id, chunk.chunk_index),
            vec![6, 7, 8, 9],
            &mut *conn,
        )
        .await
        .expect("Succeed chunk failed");

        assert!(count_chunks(&mut *conn).await.unwrap() == 0);
        assert!(count_chunks_completed(&mut *conn).await.unwrap() == 0);
        assert!(count_chunks_failed(&mut *conn).await.unwrap() == 1);
    }
}

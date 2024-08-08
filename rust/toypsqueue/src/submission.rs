use std::borrow::BorrowMut;

use sqlx::{
    pool::PoolConnection, query, query_as, Connection, Executor, Sqlite, SqliteConnection,
    SqliteExecutor,
};

use crate::chunk::{Chunk, ChunkURI};

pub type Metadata = Vec<u8>;

static ID_GENERATOR: snowflaked::sync::Generator = snowflaked::sync::Generator::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    pub id: i64,
    pub chunks_total: i64,
    pub chunks_done: i64,
    pub metadata: Option<Metadata>,
}

impl Default for Submission {
    fn default() -> Self {
        Self::new()
    }
}

impl Submission {
    pub fn new() -> Self {
        Submission {
            id: 0,
            chunks_total: 0,
            chunks_done: 0,
            metadata: None,
        }
    }

    pub fn generate_id() -> i64 {
        ID_GENERATOR.generate()
    }

    pub fn from_vec(
        chunks: Vec<ChunkURI>,
        metadata: Option<Metadata>,
    ) -> Option<(Submission, Vec<Chunk>)> {
        let submission_id = Self::generate_id();
        let len = chunks.len().try_into().ok()?;
        let submission = Submission {
            id: submission_id,
            chunks_total: len,
            chunks_done: 0,
            metadata,
        };
        let chunks = chunks
            .into_iter()
            .enumerate()
            .map(|(chunk_index, uri)| {
                // NOTE: we know that `len` fits in a u32 and therefore that the index fits in a u32 as well.
                let chunk_index = chunk_index as u32;
                Chunk::new(submission_id, chunk_index, uri)
            })
            .collect();
        Some((submission, chunks))
    }
}

pub async fn insert_submission_raw(
    submission: Submission,
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<()> {
    sqlx::query!(
        "INSERT INTO submissions (id, chunks_total, chunks_done, metadata) VALUES ($1, $2, $3, $4)",
        submission.id,
        submission.chunks_total,
        submission.chunks_done,
        submission.metadata
    )
    .execute(conn)
    .await?;
    Ok(())
}

pub async fn insert_submission(
    submission: Submission,
    chunks: impl IntoIterator<Item = Chunk>,
    conn: &mut SqliteConnection,
) -> sqlx::Result<()> {
    query!("SAVEPOINT insert_submission;")
        .execute(&mut *conn)
        .await?;
    insert_submission_raw(submission, &mut *conn).await?;
    crate::chunk::insert_many_chunks(chunks, &mut *conn).await?;
    query!("RELEASE SAVEPOINT insert_submission;")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub async fn get_submission(
    id: i64,
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<Submission> {
    query_as!(Submission, "SELECT * FROM submissions WHERE id = ?", id)
        .fetch_one(conn)
        .await
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum SubmissionStatus {
    InProgress,
    Completed,
    Failed,
}

/// TODO: Return exactly the info we have available on completed or failed
pub async fn submission_status(
    id: i64,
    conn: impl SqliteExecutor<'_>,
) -> sqlx::Result<SubmissionStatus> {
    let row = query!("
        SELECT 0 as status, chunks_done, chunks_total FROM submissions WHERE id = ?
        UNION ALL
        SELECT 1 as status, chunks_total as chunks_done, chunks_total FROM submissions_completed WHERE id = ?
        UNION ALL
        SELECT 2 as status, chunks_total as chunks_done, chunks_total FROM submissions_failed WHERE id = ?
    ",
    id,
    id,
    id,
    ).fetch_one(conn).await?;
    match row.status {
        0 => Ok(SubmissionStatus::InProgress),
        1 => Ok(SubmissionStatus::Completed),
        2 => Ok(SubmissionStatus::Failed),
        _ => Err(sqlx::Error::RowNotFound),
    }
}

pub async fn complete_submission(id: i64, conn: impl SqliteExecutor<'_>) -> sqlx::Result<()> {
    let now = chrono::prelude::Utc::now();
    query!(
        "
    SAVEPOINT complete_submission;

    INSERT INTO submissions_completed
    (id, chunks_total, metadata, completed_at)
    SELECT id, chunks_total, metadata, julianday(?) FROM submissions WHERE id = ?;

    DELETE FROM submissions WHERE id = ? RETURNING *;

    RELEASE SAVEPOINT complete_submission;
    ",
        now,
        id,
        id,
    )
    .fetch_one(conn)
    .await?;
    Ok(())
}

pub async fn fail_submission_raw(
    id: i64,
    failed_chunk_id: i64,
    conn: impl SqliteExecutor<'_>,
) -> sqlx::Result<()> {
    let now = chrono::prelude::Utc::now();

    query!(
        "
    SAVEPOINT fail_submission_raw;

    INSERT INTO submissions_failed
    (id, chunks_total, metadata, failed_at, failed_chunk_id)
    SELECT id, chunks_total, metadata, julianday(?), ? FROM submissions WHERE id = ?;

    DELETE FROM submissions WHERE id = ? RETURNING *;

    RELEASE SAVEPOINT fail_submission_raw;
    ",
        now,
        failed_chunk_id,
        id,
        id,
    )
    .fetch_one(conn)
    .await?;
    Ok(())
}

pub async fn fail_submission(
    id: i64,
    failed_chunk_id: i64,
    failure: Vec<u8>,
    conn: &mut SqliteConnection,
) -> sqlx::Result<()> {
    query!("SAVEPOINT fail_submission;")
        .execute(&mut *conn)
        .await?;
    fail_submission_raw(id, failed_chunk_id, &mut *conn).await?;
    crate::chunk::fail_chunk((id, failed_chunk_id), failure, &mut *conn).await?;
    crate::chunk::skip_remaining_chunks(id, &mut *conn).await?;
    query!("RELEASE SAVEPOINT fail_submission;")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub async fn count_submissions(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

pub async fn count_submissions_completed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions_completed;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

pub async fn count_submissions_failed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions_failed;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

#[cfg(test)]
pub mod test {
    use std::ops::DerefMut;

    use super::*;

    #[sqlx::test]
    pub async fn test_insert_submission(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();

        assert!(count_submissions(&mut *conn).await.unwrap() == 0);

        let (submission, chunks) =
            Submission::from_vec(vec!["foo".into(), "bar".into(), "baz".into()], None).unwrap();
        insert_submission(submission, chunks, &mut *conn)
            .await
            .expect("insertion failed");

        assert!(count_submissions(&db).await.unwrap() == 1);
    }

    #[sqlx::test]
    pub async fn test_get_submission(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let (submission, chunks) =
            Submission::from_vec(vec!["foo".into(), "bar".into(), "baz".into()], None).unwrap();
        insert_submission(submission.clone(), chunks, &mut *conn)
            .await
            .expect("insertion failed");

        let fetched_submission = get_submission(submission.id, &mut *conn).await.unwrap();
        assert!(fetched_submission == submission);
    }

    #[sqlx::test]
    pub async fn test_complete_submission(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let (submission, chunks) =
            Submission::from_vec(vec!["foo".into(), "bar".into(), "baz".into()], None).unwrap();
        insert_submission(submission.clone(), chunks, &mut *conn)
            .await
            .expect("insertion failed");

        complete_submission(submission.id, &mut *conn)
            .await
            .unwrap();
        assert!(count_submissions(&mut *conn).await.unwrap() == 0);
        assert!(count_submissions_completed(&mut *conn).await.unwrap() == 1);
        assert!(count_submissions_failed(&mut *conn).await.unwrap() == 0);
    }

    #[sqlx::test]
    pub async fn test_fail_submission_raw(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let (submission, chunks) =
            Submission::from_vec(vec!["foo".into(), "bar".into(), "baz".into()], None).unwrap();
        insert_submission(submission.clone(), chunks, &mut *conn)
            .await
            .expect("insertion failed");
        let mut conn = db.acquire().await.unwrap();

        fail_submission(submission.id, 1, vec![1, 2, 3], &mut *conn)
            .await
            .unwrap();
        assert!(count_submissions(&mut *conn).await.unwrap() == 0);
        assert!(count_submissions_completed(&mut *conn).await.unwrap() == 0);
        assert!(count_submissions_failed(&mut *conn).await.unwrap() == 1);
    }
}

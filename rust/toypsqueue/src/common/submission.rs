use chrono::NaiveDateTime;
use sqlx::{query, query_as, Executor, Sqlite, SqliteConnection, SqliteExecutor};

use super::chunk;
use super::chunk::Chunk;

pub type Metadata = Vec<u8>;

static ID_GENERATOR: snowflaked::sync::Generator = snowflaked::sync::Generator::new(0);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Submission {
    pub id: i64,
    pub chunks_total: i64,
    pub chunks_done: i64,
    pub metadata: Option<Metadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionCompleted {
    pub id: i64,
    pub chunks_done: i64,
    pub metadata: Option<Metadata>,
    pub completed_at: NaiveDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionFailed {
    pub id: i64,
    pub chunks_total: i64,
    pub metadata: Option<Metadata>,
    pub failed_at: NaiveDateTime,
    pub failed_chunk_id: i64,
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
        chunks: Vec<chunk::Content>,
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
    super::chunk::insert_many_chunks(chunks, &mut *conn).await?;
    query!("RELEASE SAVEPOINT insert_submission;")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub async fn insert_submission_from_chunks(
    metadata: Option<Metadata>,
    chunks_contents: Vec<chunk::Content>,
    conn: &mut SqliteConnection,
) -> sqlx::Result<i64> {
    let submission_id = Submission::generate_id();
    let submission = Submission {
        id: submission_id,
        chunks_total: chunks_contents.len() as i64,
        chunks_done: 0,
        metadata,
    };
    let iter = chunks_contents
        .into_iter()
        .enumerate()
        .map(|(chunk_index, uri)| Chunk::new(submission_id, chunk_index as u32, uri));
    insert_submission(submission, iter, conn).await?;
    Ok(submission_id)
}

pub async fn get_submission(
    id: i64,
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<Submission> {
    query_as!(Submission, "SELECT * FROM submissions WHERE id = ?", id)
        .fetch_one(conn)
        .await
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubmissionStatusTag {
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubmissionStatus {
    InProgress(Submission),
    Completed(SubmissionCompleted),
    Failed(SubmissionFailed),
}

/// TODO: Return exactly the info we have available on completed or failed
pub async fn submission_status(
    id: i64,
    conn: impl SqliteExecutor<'_>,
) -> sqlx::Result<SubmissionStatus> {
    let row = query!("
        SELECT 0 as status, id, chunks_done, chunks_total, metadata, NULL as failed_chunk_id, NULL as failed_at, NULL as completed_at FROM submissions WHERE id = ?
        UNION ALL
        SELECT 1 as status, id, NULL as chunks_done, chunks_total, metadata, NULL as failed_chunk_id, NULL as failed_at, completed_at FROM submissions_completed WHERE id = ?
        UNION ALL
        SELECT 2 as status, id, NULL as chunks_done, chunks_total, metadata, failed_chunk_id, failed_at, NULL as completed_at FROM submissions_failed WHERE id = ?
    ",
    id,
    id,
    id,
    ).fetch_one(conn).await?;
    match row.status {
        // TODO: Cleaner error handling than unwrap_or_default
        0 => Ok(SubmissionStatus::InProgress(Submission {
            id: row.id,
            chunks_total: row.chunks_total,
            chunks_done: row.chunks_done.unwrap_or_default(),
            metadata: row.metadata,
        })),
        1 => Ok(SubmissionStatus::Completed(SubmissionCompleted {
            id: row.id,
            chunks_done: row.chunks_done.unwrap_or_default(),
            metadata: row.metadata,
            completed_at: row.completed_at.unwrap_or_default(),
        })),
        2 => Ok(SubmissionStatus::Failed(SubmissionFailed {
            id: row.id,
            chunks_total: row.chunks_total,
            metadata: row.metadata,
            failed_chunk_id: row.failed_chunk_id.unwrap_or_default(),
            failed_at: row.failed_at.unwrap_or_default(),
        })),
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
    super::chunk::fail_chunk((id, failed_chunk_id), failure, &mut *conn).await?;
    super::chunk::skip_remaining_chunks(id, &mut *conn).await?;
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

    use super::*;

    #[sqlx::test]
    pub async fn test_insert_submission(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();

        assert!(count_submissions(&mut *conn).await.unwrap() == 0);

        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
        )
        .unwrap();
        insert_submission(submission, chunks, &mut conn)
            .await
            .expect("insertion failed");

        assert!(count_submissions(&db).await.unwrap() == 1);
    }

    #[sqlx::test]
    pub async fn test_get_submission(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
        )
        .unwrap();
        insert_submission(submission.clone(), chunks, &mut conn)
            .await
            .expect("insertion failed");

        let fetched_submission = get_submission(submission.id, &mut *conn).await.unwrap();
        assert!(fetched_submission == submission);
    }

    #[sqlx::test]
    pub async fn test_complete_submission(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
        )
        .unwrap();
        insert_submission(submission.clone(), chunks, &mut conn)
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
        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
        )
        .unwrap();
        insert_submission(submission.clone(), chunks, &mut conn)
            .await
            .expect("insertion failed");
        let mut conn = db.acquire().await.unwrap();

        fail_submission(submission.id, 1, vec![1, 2, 3], &mut conn)
            .await
            .unwrap();
        assert!(count_submissions(&mut *conn).await.unwrap() == 0);
        assert!(count_submissions_completed(&mut *conn).await.unwrap() == 0);
        assert!(count_submissions_failed(&mut *conn).await.unwrap() == 1);
    }
}

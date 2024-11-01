use std::fmt::Display;
use std::time::Duration;

use chrono::{DateTime, Utc};
#[cfg(feature = "server-logic")]
use sqlx::{query, query_as, Connection, Executor, Sqlite, SqliteConnection, SqliteExecutor};

#[cfg(feature = "server-logic")]
use super::chunk::ChunkIndex;
use super::chunk::{self, Chunk};

#[cfg(feature = "server-logic")]
use super::errors::{DatabaseError, E, SubmissionNotFound};
use snowflaked::Snowflake;

pub type Metadata = Vec<u8>;

static ID_GENERATOR: snowflaked::sync::Generator = snowflaked::sync::Generator::new(0);

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct SubmissionId(i64);

impl Display for SubmissionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Debug for SubmissionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmissionId")
            .field("id", &self.0)
            .field("timestamp", &self.timestamp())
            .finish()
    }
}

#[cfg(feature = "server-logic")]
impl<'q> sqlx::Encode<'q, Sqlite> for SubmissionId {
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

impl From<i64> for SubmissionId {
    fn from(value: i64) -> Self {
        SubmissionId(value)
    }
}

impl From<SubmissionId> for i64 {
    fn from(value: SubmissionId) -> Self {
        value.0
    }
}

impl From<&SubmissionId> for std::time::SystemTime {
    fn from(val: &SubmissionId) -> Self {
        val.system_time()
    }
}

#[cfg(feature = "server-logic")]
impl sqlx::Type<Sqlite> for SubmissionId {
    fn compatible(ty: &<Sqlite as sqlx::Database>::TypeInfo) -> bool {
        <i64 as sqlx::Type<Sqlite>>::compatible(ty)
    }
    fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
        <i64 as sqlx::Type<Sqlite>>::type_info()
    }
}

impl SubmissionId {
    pub fn system_time(self) -> std::time::SystemTime {
        let unix_timestamp_ms = self.0.timestamp();
        let unix_timestamp = Duration::from_millis(unix_timestamp_ms);
        ID_GENERATOR
            .epoch()
            .checked_add(unix_timestamp)
            .expect("Invalid timestamp extracted from snowflake ID")
    }

    pub fn timestamp(self) -> chrono::DateTime<Utc> {
        self.system_time().into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Submission {
    pub id: SubmissionId,
    pub prefix: Option<String>,
    pub chunks_total: i64,
    pub chunks_done: i64,
    pub metadata: Option<Metadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionCompleted {
    pub id: SubmissionId,
    pub prefix: Option<String>,
    pub chunks_total: i64,
    pub metadata: Option<Metadata>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionFailed {
    pub id: SubmissionId,
    pub prefix: Option<String>,
    pub chunks_total: i64,
    pub metadata: Option<Metadata>,
    pub failed_at: DateTime<Utc>,
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
            id: SubmissionId(0),
            prefix: None,
            chunks_total: 0,
            chunks_done: 0,
            metadata: None,
        }
    }

    pub fn generate_id() -> SubmissionId {
        SubmissionId(ID_GENERATOR.generate())
    }

    pub fn from_vec(
        chunks: Vec<chunk::Content>,
        metadata: Option<Metadata>,
    ) -> Option<(Submission, Vec<Chunk>)> {
        let submission_id = Self::generate_id();
        let len = chunks.len().try_into().ok()?;
        let submission = Submission {
            id: submission_id,
            prefix: None,
            chunks_total: len,
            chunks_done: 0,
            metadata,
        };
        let chunks = chunks
            .into_iter()
            .enumerate()
            .map(|(chunk_index, uri)| {
                // NOTE: we know that `len` fits in the unsigned 63-bit part of a i64 and therefore that the index fits it as well.
                let chunk_index = (chunk_index as i64).into();
                Chunk::new(submission_id, chunk_index, uri)
            })
            .collect();
        Some((submission, chunks))
    }
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn insert_submission_raw(
    submission: Submission,
    conn: impl Executor<'_, Database = Sqlite>,
) -> Result<(), DatabaseError> {
    sqlx::query!(
        "
        INSERT INTO submissions (id, prefix, chunks_total, chunks_done, metadata)
        VALUES ($1, $2, $3, $4, $5)
        ",
        submission.id,
        submission.prefix,
        submission.chunks_total,
        submission.chunks_done,
        submission.metadata
    )
    .execute(conn)
    .await?;
    Ok(())
}

#[cfg(feature = "server-logic")]
#[tracing::instrument(skip(chunks))]
pub async fn insert_submission<Iter>(
    submission: Submission,
    chunks: Iter,
    conn: &mut SqliteConnection,
) -> Result<(), DatabaseError>
where
    Iter: IntoIterator<Item = Chunk> + Send + Sync + 'static,
    <Iter as IntoIterator>::IntoIter: Send + Sync + 'static,
{
    conn.transaction(|tx| {
        Box::pin(async move {
            insert_submission_raw(submission, &mut **tx).await?;
            super::chunk::insert_many_chunks(chunks, &mut **tx).await?;
            Ok(())
        })
    })
    .await
}

#[cfg(feature = "server-logic")]
#[tracing::instrument(skip(metadata, chunks_contents, conn))]
pub async fn insert_submission_from_chunks(
    prefix: Option<String>,
    chunks_contents: Vec<chunk::Content>,
    metadata: Option<Metadata>,
    conn: &mut SqliteConnection,
) -> Result<SubmissionId, DatabaseError> {
    let submission_id = Submission::generate_id();
    let submission = Submission {
        id: submission_id,
        prefix,
        chunks_total: chunks_contents.len() as i64,
        chunks_done: 0,
        metadata,
    };
    let iter = chunks_contents
        .into_iter()
        .enumerate()
        .map(move |(chunk_index, uri)| Chunk::new(submission_id, (chunk_index as i64).into(), uri));
    insert_submission(submission, iter, conn).await?;
    Ok(submission_id)
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn get_submission(
    id: SubmissionId,
    conn: impl Executor<'_, Database = Sqlite>,
) -> Result<Submission, E<DatabaseError, SubmissionNotFound>> {
    let submission = query_as!(Submission, "SELECT * FROM submissions WHERE id = ?", id)
        .fetch_one(conn)
        .await?;
    Ok(submission)
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

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn submission_status(
    id: SubmissionId,
    conn: impl SqliteExecutor<'_>,
) -> Result<Option<SubmissionStatus>, DatabaseError> {
    let maybe_row = query!("
        SELECT 0 as status, id, prefix, chunks_done, chunks_total, metadata, NULL as failed_chunk_id, NULL as failed_at, NULL as completed_at FROM submissions WHERE id = ?
        UNION ALL
        SELECT 1 as status, id, prefix, NULL as chunks_done, chunks_total, metadata, NULL as failed_chunk_id, NULL as failed_at, completed_at FROM submissions_completed WHERE id = ?
        UNION ALL
        SELECT 2 as status, id, prefix, NULL as chunks_done, chunks_total, metadata, failed_chunk_id, failed_at, NULL as completed_at FROM submissions_failed WHERE id = ?
    ",
    id,
    id,
    id,
    ).fetch_one(conn).await;
    match maybe_row {
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(other) => Err(other.into()),
        Ok(row) => match row.status {
            // TODO: Cleaner error handling than unwrap_or_default
            0 => Ok(Some(SubmissionStatus::InProgress(Submission {
                id: row.id.into(),
                prefix: row.prefix,
                chunks_total: row.chunks_total,
                chunks_done: row.chunks_done.unwrap_or_default(),
                metadata: row.metadata,
            }))),
            1 => Ok(Some(SubmissionStatus::Completed(SubmissionCompleted {
                id: row.id.into(),
                prefix: row.prefix,
                chunks_total: row.chunks_total,
                metadata: row.metadata,
                completed_at: row.completed_at.unwrap_or_default().and_utc(),
            }))),
            2 => Ok(Some(SubmissionStatus::Failed(SubmissionFailed {
                id: row.id.into(),
                prefix: row.prefix,
                chunks_total: row.chunks_total,
                metadata: row.metadata,
                failed_chunk_id: row.failed_chunk_id.unwrap_or_default(),
                failed_at: row.failed_at.unwrap_or_default().and_utc(),
            }))),
            idx => Err(sqlx::Error::ColumnIndexOutOfBounds {
                index: 0,
                len: idx as usize,
            }
            .into()),
        },
    }
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
/// Completes the submission, iff all chunks have been completed.
pub async fn maybe_complete_submission(
    id: SubmissionId,
    conn: &mut SqliteConnection,
) -> Result<bool, E<DatabaseError, SubmissionNotFound>> {
    use sqlx::Connection;
    conn.transaction(|tx| {
        Box::pin(async move {
            let submission = get_submission(id, &mut **tx).await?;

            if submission.chunks_done == submission.chunks_total {
                complete_submission_raw(id, &mut **tx).await?;
                Ok(true)
            } else {
                Ok(false)
            }
        })
    })
    .await
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
/// TODO: Should only do the actual work iff chunks_done === chunks_total.
pub async fn complete_submission_raw(
    id: SubmissionId,
    conn: impl SqliteExecutor<'_>,
) -> Result<(), E<DatabaseError, SubmissionNotFound>> {
    let now = chrono::prelude::Utc::now();
    query!(
        "
    SAVEPOINT complete_submission_raw;

    INSERT INTO submissions_completed
    (id, chunks_total, prefix, metadata, completed_at)
    SELECT id, chunks_total, prefix, metadata, julianday(?) FROM submissions WHERE id = ?;

    DELETE FROM submissions WHERE id = ? RETURNING *;

    RELEASE SAVEPOINT complete_submission_raw;
    ",
        now,
        id,
        id,
    )
    .fetch_one(conn)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => E::R(SubmissionNotFound(id)),
        e => E::L(DatabaseError::from(e)),
    })?;
    Ok(())
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn fail_submission_raw(
    id: SubmissionId,
    failed_chunk_id: ChunkIndex,
    conn: impl SqliteExecutor<'_>,
) -> sqlx::Result<()> {
    let now = chrono::prelude::Utc::now();

    query!(
        "
    INSERT INTO submissions_failed
    (id, chunks_total, prefix, metadata, failed_at, failed_chunk_id)
    SELECT id, chunks_total, prefix, metadata, julianday(?), ? FROM submissions WHERE id = ?;

    DELETE FROM submissions WHERE id = ? RETURNING *;
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

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn fail_submission(
    id: SubmissionId,
    failed_chunk_index: ChunkIndex,
    failure: String,
    conn: &mut SqliteConnection,
) -> sqlx::Result<()> {
    conn.transaction(|tx| {
        Box::pin(async move {
            fail_submission_raw(id, failed_chunk_index, &mut **tx).await?;
            super::chunk::move_chunk_to_failed_chunks((id, failed_chunk_index), failure, &mut **tx)
                .await?;
            super::chunk::skip_remaining_chunks(id, &mut **tx).await?;
            Ok(())
        })
    })
    .await
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn count_submissions(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn count_submissions_completed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions_completed;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn count_submissions_failed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions_failed;")
        .fetch_one(db)
        .await?;
    Ok(count.count)
}

/// Transactionally removes all completed/failed submissions (and all their chunks) older than a given timestamp from the database.
///
/// Submissions/chunks that are neither failed nor completed are not touched.
#[cfg(feature = "server-logic")]
#[tracing::instrument]
pub async fn cleanup_old(
    conn: &mut SqliteConnection,
    older_than: DateTime<Utc>,
) -> sqlx::Result<()> {
    conn.transaction(|tx| {
        Box::pin(async move {
            query!(
                "DELETE FROM submissions_completed WHERE completed_at < julianday(?);",
                older_than
            )
            .execute(&mut **tx)
            .await?;
            query!(
                "DELETE FROM submissions_failed WHERE failed_at < julianday(?);",
                older_than
            )
            .execute(&mut **tx)
            .await?;

            query!(
                "DELETE FROM chunks_completed WHERE completed_at < julianday(?);",
                older_than
            )
            .execute(&mut **tx)
            .await?;
            query!(
                "DELETE FROM chunks_failed WHERE failed_at < julianday(?);",
                older_than
            )
            .execute(&mut **tx)
            .await?;
            Ok(())
        })
    })
    .await
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {

    use chrono::Utc;

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
    pub async fn test_complete_submission_raw(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
        )
        .unwrap();
        insert_submission(submission.clone(), chunks, &mut conn)
            .await
            .expect("insertion failed");

        complete_submission_raw(submission.id, &mut *conn)
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

        fail_submission(submission.id, 1.into(), "Boom!".to_string(), &mut conn)
            .await
            .unwrap();
        assert!(count_submissions(&mut *conn).await.unwrap() == 0);
        assert!(count_submissions_completed(&mut *conn).await.unwrap() == 0);
        assert!(count_submissions_failed(&mut *conn).await.unwrap() == 1);
    }

    #[sqlx::test]
    pub async fn test_cleanup_old(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();

        let chunks_contents = vec![Some("foo".into()), Some("bar".into()), Some("baz".into())];
        let old_one = insert_submission_from_chunks(None, chunks_contents.clone(), None, &mut conn)
            .await
            .unwrap();
        let old_two = insert_submission_from_chunks(None, chunks_contents.clone(), None, &mut conn)
            .await
            .unwrap();
        let old_three =
            insert_submission_from_chunks(None, chunks_contents.clone(), None, &mut conn)
                .await
                .unwrap();
        let old_four_unfailed =
            insert_submission_from_chunks(None, chunks_contents.clone(), None, &mut conn)
                .await
                .unwrap();

        fail_submission(old_one, 0.into(), "Broken one".into(), &mut conn)
            .await
            .unwrap();
        fail_submission(old_two, 0.into(), "Broken two".into(), &mut conn)
            .await
            .unwrap();
        fail_submission(old_three, 0.into(), "Broken three".into(), &mut conn)
            .await
            .unwrap();

        let cutoff_timestamp = Utc::now();

        let too_new_one =
            insert_submission_from_chunks(None, chunks_contents.clone(), None, &mut conn)
                .await
                .unwrap();
        let _too_new_two_unfailed =
            insert_submission_from_chunks(None, chunks_contents.clone(), None, &mut conn)
                .await
                .unwrap();
        let too_new_three =
            insert_submission_from_chunks(None, chunks_contents.clone(), None, &mut conn)
                .await
                .unwrap();

        fail_submission(too_new_one, 0.into(), "Broken new one".into(), &mut conn)
            .await
            .unwrap();
        fail_submission(
            too_new_three,
            0.into(),
            "Broken new three".into(),
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(count_submissions_failed(&mut *conn).await.unwrap(), 5);

        cleanup_old(&mut conn, cutoff_timestamp).await.unwrap();

        assert_eq!(count_submissions_failed(&mut *conn).await.unwrap(), 2);

        let _sub1 = submission_status(old_four_unfailed, &mut *conn).await;
        let _sub2 = submission_status(old_four_unfailed, &mut *conn).await;
    }
}

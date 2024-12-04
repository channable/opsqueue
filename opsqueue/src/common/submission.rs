use std::fmt::Display;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ux_serde::u63;

use super::chunk::{self, Chunk};
use super::chunk::{ChunkCount, ChunkIndex};

pub type Metadata = Vec<u8>;

static ID_GENERATOR: snowflaked::sync::Generator = snowflaked::sync::Generator::new(0);

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct SubmissionId(u63);

impl Display for SubmissionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Debug for SubmissionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SubmissionId").field(&self.0).finish()
    }
}

impl From<u63> for SubmissionId {
    fn from(value: u63) -> Self {
        SubmissionId(value)
    }
}

impl From<SubmissionId> for u63 {
    fn from(value: SubmissionId) -> Self {
        value.0
    }
}

impl From<SubmissionId> for i64 {
    fn from(value: SubmissionId) -> Self {
        let inner: u64 = value.0.into();
        // Guaranteed to fit positive signed range
        inner as i64
    }
}

impl From<SubmissionId> for u64 {
    fn from(value: SubmissionId) -> Self {
        value.0.into()
    }
}

impl TryFrom<u64> for SubmissionId {
    type Error = crate::common::errors::TryFromIntError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value > u63::MAX.into() {
            return Err(crate::common::errors::TryFromIntError(()));
        }
        Ok(Self(u63::new(value)))
    }
}

impl From<&SubmissionId> for std::time::SystemTime {
    fn from(val: &SubmissionId) -> Self {
        val.system_time()
    }
}

impl SubmissionId {
    pub fn system_time(self) -> std::time::SystemTime {
        use snowflaked::Snowflake;
        let inner: u64 = self.0.into();

        let unix_timestamp_ms = inner.timestamp();
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
    pub chunks_total: ChunkCount,
    pub chunks_done: ChunkCount,
    pub metadata: Option<Metadata>,
    pub otel_trace_carrier: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionCompleted {
    pub id: SubmissionId,
    pub prefix: Option<String>,
    pub chunks_total: ChunkCount,
    pub metadata: Option<Metadata>,
    pub completed_at: DateTime<Utc>,
    pub otel_trace_carrier: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionFailed {
    pub id: SubmissionId,
    pub prefix: Option<String>,
    pub chunks_total: ChunkCount,
    pub metadata: Option<Metadata>,
    pub failed_at: DateTime<Utc>,
    pub failed_chunk_id: ChunkIndex,
    pub otel_trace_carrier: String,
}

// #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
// pub enum SubmissionStatusTag {
//     InProgress,
//     Completed,
//     Failed,
// }

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubmissionStatus {
    InProgress(Submission),
    Completed(SubmissionCompleted),
    Failed(SubmissionFailed),
}

impl Default for Submission {
    fn default() -> Self {
        Self::new()
    }
}

impl Submission {
    pub fn new() -> Self {
        let otel_trace_carrier = crate::tracing::current_context_to_json();
        Submission {
            id: SubmissionId(u63::new(0)),
            prefix: None,
            chunks_total: ChunkCount::zero(),
            chunks_done: ChunkCount::zero(),
            metadata: None,
            otel_trace_carrier,
        }
    }

    pub fn generate_id() -> SubmissionId {
        let inner: u64 = ID_GENERATOR.generate();
        SubmissionId(u63::new(inner))
    }

    pub fn from_vec(
        chunks: Vec<chunk::Content>,
        metadata: Option<Metadata>,
    ) -> Option<(Submission, Vec<Chunk>)> {
        let submission_id = Self::generate_id();
        let len = ChunkCount::new(u64::try_from(chunks.len()).ok()?).ok()?;
        let otel_trace_carrier = crate::tracing::current_context_to_json();
        let submission = Submission {
            id: submission_id,
            prefix: None,
            chunks_total: len,
            chunks_done: ChunkCount::zero(),
            metadata,
            otel_trace_carrier
        };
        let chunks = chunks
            .into_iter()
            .enumerate()
            .map(|(chunk_index, uri)| {
                // NOTE: we know that `len` fits in the unsigned 63-bit part of a i64 and therefore that the index fits it as well.
                let chunk_index = ChunkIndex::from(u63::new(chunk_index as u64));
                Chunk::new(submission_id, chunk_index, uri)
            })
            .collect();
        Some((submission, chunks))
    }
}

#[cfg(feature = "server-logic")]
pub mod db {
    use crate::common::errors::{DatabaseError, SubmissionNotFound, E};
    #[cfg(feature = "server-logic")]
    use sqlx::{query, query_as, Connection, Executor, Sqlite, SqliteConnection, SqliteExecutor};

    #[cfg(feature = "server-logic")]
    use axum_prometheus::metrics::{counter, histogram};

    use super::*;

    impl<'q> sqlx::Encode<'q, Sqlite> for SubmissionId {
        fn encode_by_ref(
            &self,
            buf: &mut <Sqlite as sqlx::Database>::ArgumentBuffer<'q>,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            <i64 as sqlx::Encode<'q, Sqlite>>::encode_by_ref(&i64::from(*self), buf)
        }

        fn encode(
            self,
            buf: &mut <Sqlite as sqlx::Database>::ArgumentBuffer<'q>,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError>
        where
            Self: Sized,
        {
            <i64 as sqlx::Encode<'q, Sqlite>>::encode(self.into(), buf)
        }
    }

    impl<'q> sqlx::Decode<'q, Sqlite> for SubmissionId {
        fn decode(
            value: <Sqlite as sqlx::Database>::ValueRef<'q>,
        ) -> Result<Self, sqlx::error::BoxDynError> {
            let inner = <u64 as sqlx::Decode<'q, Sqlite>>::decode(value)?;
            let x = SubmissionId::try_from(inner)?;
            Ok(x)
        }
    }

    impl sqlx::Type<Sqlite> for SubmissionId {
        fn compatible(ty: &<Sqlite as sqlx::Database>::TypeInfo) -> bool {
            <u64 as sqlx::Type<Sqlite>>::compatible(ty)
        }
        fn type_info() -> <Sqlite as sqlx::Database>::TypeInfo {
            <u64 as sqlx::Type<Sqlite>>::type_info()
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
        INSERT INTO submissions (id, prefix, chunks_total, chunks_done, metadata, otel_trace_carrier)
        VALUES ($1, $2, $3, $4, $5, $6)
        ",
            submission.id,
            submission.prefix,
            submission.chunks_total,
            submission.chunks_done,
            submission.metadata,
            submission.otel_trace_carrier
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
        use axum_prometheus::metrics::{counter, gauge};
        let chunks_total = submission.chunks_total.into();
        tracing::debug!("Inserting submission {}", submission.id);

        let res = conn
            .transaction(|tx| {
                Box::pin(async move {
                    insert_submission_raw(submission, &mut **tx).await?;
                    super::chunk::db::insert_many_chunks(chunks, &mut **tx).await?;
                    Ok(())
                })
            })
            .await;

        counter!(crate::prometheus::SUBMISSIONS_TOTAL_COUNTER).increment(1);
        counter!(crate::prometheus::CHUNKS_TOTAL_COUNTER).increment(chunks_total);
        gauge!(crate::prometheus::CHUNKS_BACKLOG_GAUGE).increment(chunks_total as f64);
        res
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
        let len = chunks_contents.len().try_into().expect("Vector length larger than u63 range. Unlikely because of RAM constraints but theoretically possible");
        let otel_trace_carrier = crate::tracing::current_context_to_json();
        let submission = Submission {
            id: submission_id,
            prefix,
            chunks_total: len,
            chunks_done: ChunkCount::zero(),
            metadata,
            otel_trace_carrier,
        };
        let iter = chunks_contents
            .into_iter()
            .enumerate()
            .map(move |(chunk_index, uri)| {
                // NOTE: Since `len` fits in a u64, these indexes by definition must too!
                Chunk::new(submission_id, chunk_index.try_into().unwrap(), uri)
            });
        insert_submission(submission, iter, conn).await?;
        Ok(submission_id)
    }

    #[cfg(feature = "server-logic")]
    #[tracing::instrument]
    pub async fn get_submission(
        id: SubmissionId,
        conn: impl Executor<'_, Database = Sqlite>,
    ) -> Result<Submission, E<DatabaseError, SubmissionNotFound>> {
        let submission = query_as!(Submission, 
            r#"
            SELECT id AS "id: SubmissionId"
            , prefix
            , chunks_total AS "chunks_total: ChunkCount"
            , chunks_done AS "chunks_done: ChunkCount"
            , metadata 
            , otel_trace_carrier
            FROM submissions WHERE id = ?
            "#, id
        )
        .fetch_one(conn)
        .await?;
        Ok(submission)
    }

    #[tracing::instrument]
    pub async fn lookup_id_by_prefix(
        prefix: &str,
        conn: &mut SqliteConnection,
    ) -> Result<Option<SubmissionId>, DatabaseError> {
        let row = query!(
            r#"
            SELECT id AS "id: SubmissionId" FROM submissions WHERE prefix = ?
            UNION ALL
            SELECT id AS "id: SubmissionId" FROM submissions_completed WHERE prefix = ?
            UNION ALL
            SELECT id AS "id: SubmissionId" FROM submissions_failed WHERE prefix = ?
            "#,
            prefix,
            prefix,
            prefix
        )
        .fetch_optional(conn)
        .await?;
        Ok(row.map(|row| row.id))
    }

    #[cfg(feature = "server-logic")]
    #[tracing::instrument]
    pub async fn submission_status(
        id: SubmissionId,
        conn: &mut SqliteConnection,
    ) -> Result<Option<SubmissionStatus>, DatabaseError> {
        // NOTE: The order is important here; a concurrent writer could move a submission
        // from InProgress to Completed/Failed in-between the queries.
        let submission = query_as!(
            Submission,
            r#"
        SELECT
              id AS "id: SubmissionId"
            , prefix
            , chunks_total AS "chunks_total: ChunkCount"
            , chunks_done AS "chunks_done: ChunkCount"
            , metadata
            , otel_trace_carrier
        FROM submissions WHERE id = ?
        "#,
            id
        )
        .fetch_optional(&mut *conn)
        .await?;
        if let Some(submission) = submission {
            return Ok(Some(SubmissionStatus::InProgress(submission)));
        }

        let completed_submission = query_as!(
            SubmissionCompleted,
            r#"
        SELECT
            id AS "id: SubmissionId"
            , prefix
            , chunks_total AS "chunks_total: ChunkCount"
            , metadata
            , completed_at AS "completed_at: DateTime<Utc>"
            , otel_trace_carrier
        FROM submissions_completed WHERE id = ?
        "#,
            id
        )
        .fetch_optional(&mut *conn)
        .await?;
        if let Some(completed_submission) = completed_submission {
            return Ok(Some(SubmissionStatus::Completed(completed_submission)));
        }

        let failed_submission = query_as!(
            SubmissionFailed,
            r#"
        SELECT
              id AS "id: SubmissionId"
            , prefix
            , chunks_total AS "chunks_total: ChunkCount"
            , metadata
            , failed_at AS "failed_at: DateTime<Utc>"
            , failed_chunk_id AS "failed_chunk_id: ChunkIndex"
            , otel_trace_carrier
        FROM submissions_failed WHERE id = ?
        "#,
            id
        )
        .fetch_optional(&mut *conn)
        .await?;
        if let Some(failed_submission) = failed_submission {
            return Ok(Some(SubmissionStatus::Failed(failed_submission)));
        }

        Ok(None)
    }

    #[cfg(feature = "server-logic")]
    #[tracing::instrument]
    /// Completes the submission, iff all chunks have been completed.
    ///
    /// Do not call directly! MUST be called inside a transaction.
    pub async fn maybe_complete_submission(
        id: SubmissionId,
        conn: &mut SqliteConnection,
    ) -> Result<bool, E<DatabaseError, SubmissionNotFound>> {
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
        counter!(crate::prometheus::SUBMISSIONS_COMPLETED_COUNTER).increment(1);
        histogram!(crate::prometheus::SUBMISSIONS_DURATION_COMPLETE_HISTOGRAM).record(
            crate::prometheus::time_delta_as_f64(Utc::now() - id.timestamp()),
        );
        tracing::debug!("Completed submission {id}, timestamp {}", id.timestamp());
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
        counter!(crate::prometheus::SUBMISSIONS_FAILED_COUNTER).increment(1);
        histogram!(crate::prometheus::SUBMISSIONS_DURATION_FAIL_HISTOGRAM).record(
            crate::prometheus::time_delta_as_f64(Utc::now() - id.timestamp()),
        );

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
        use sqlx::Connection;
        conn.transaction(|tx| {
            Box::pin(async move { fail_submission_notx(id, failed_chunk_index, failure, tx).await })
        })
        .await
    }

    /// Do not call directly! Must be called inside a transaction.
    pub async fn fail_submission_notx(
        id: SubmissionId,
        failed_chunk_index: ChunkIndex,
        failure: String,
        conn: &mut SqliteConnection,
    ) -> sqlx::Result<()> {
        fail_submission_raw(id, failed_chunk_index, &mut *conn).await?;
        super::chunk::db::move_chunk_to_failed_chunks(
            (id, failed_chunk_index).into(),
            failure,
            &mut *conn,
        )
        .await?;
        super::chunk::db::skip_remaining_chunks(id, &mut *conn).await?;
        Ok(())
    }

    #[cfg(feature = "server-logic")]
    #[tracing::instrument]
    pub async fn count_submissions(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<usize> {
        let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions;")
            .fetch_one(db)
            .await?;
        Ok(count.count as usize)
    }

    #[cfg(feature = "server-logic")]
    #[tracing::instrument]
    pub async fn count_submissions_completed(
        db: impl sqlx::SqliteExecutor<'_>,
    ) -> sqlx::Result<usize> {
        let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions_completed;")
            .fetch_one(db)
            .await?;
        Ok(count.count as usize)
    }

    #[cfg(feature = "server-logic")]
    #[tracing::instrument]
    pub async fn count_submissions_failed(
        db: impl sqlx::SqliteExecutor<'_>,
    ) -> sqlx::Result<usize> {
        let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions_failed;")
            .fetch_one(db)
            .await?;
        Ok(count.count as usize)
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
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {

    use chrono::Utc;

    use super::db::*;
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

        fail_submission(
            submission.id,
            u63::new(1).into(),
            "Boom!".to_string(),
            &mut conn,
        )
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

        fail_submission(old_one, u63::new(0).into(), "Broken one".into(), &mut conn)
            .await
            .unwrap();
        fail_submission(old_two, u63::new(0).into(), "Broken two".into(), &mut conn)
            .await
            .unwrap();
        fail_submission(
            old_three,
            u63::new(0).into(),
            "Broken three".into(),
            &mut conn,
        )
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

        fail_submission(
            too_new_one,
            u63::new(0).into(),
            "Broken new one".into(),
            &mut conn,
        )
        .await
        .unwrap();
        fail_submission(
            too_new_three,
            u63::new(0).into(),
            "Broken new three".into(),
            &mut conn,
        )
        .await
        .unwrap();

        assert_eq!(count_submissions_failed(&mut *conn).await.unwrap(), 5);

        let mut conn2 = db.acquire().await.unwrap();
        cleanup_old(&mut conn2, cutoff_timestamp).await.unwrap();

        assert_eq!(count_submissions_failed(&mut *conn).await.unwrap(), 2);

        let _sub1 = submission_status(old_four_unfailed, &mut conn).await;
        let _sub2 = submission_status(old_four_unfailed, &mut conn).await;
    }
}

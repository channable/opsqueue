//! Dealing with `Submission`s: Collections of (`Chunks`s of) operations.
use std::fmt::Display;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ux_serde::u63;

use super::chunk::{self, Chunk, ChunkFailed, ChunkSize};
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
    pub chunk_size: ChunkSize,
    pub metadata: Option<Metadata>,
    pub otel_trace_carrier: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionCompleted {
    pub id: SubmissionId,
    pub prefix: Option<String>,
    pub chunks_total: ChunkCount,
    pub chunk_size: ChunkSize,
    pub metadata: Option<Metadata>,
    pub completed_at: DateTime<Utc>,
    pub otel_trace_carrier: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionFailed {
    pub id: SubmissionId,
    pub prefix: Option<String>,
    pub chunks_total: ChunkCount,
    pub chunk_size: ChunkSize,
    pub metadata: Option<Metadata>,
    pub failed_at: DateTime<Utc>,
    pub failed_chunk_id: ChunkIndex,
    pub otel_trace_carrier: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubmissionStatus {
    InProgress(Submission),
    Completed(SubmissionCompleted),
    Failed(SubmissionFailed, ChunkFailed),
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
            chunk_size: ChunkSize::default(),
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
        chunk_size: ChunkSize,
    ) -> Option<(Submission, Vec<Chunk>)> {
        let submission_id = Self::generate_id();
        let len = ChunkCount::new(u64::try_from(chunks.len()).ok()?).ok()?;
        let otel_trace_carrier = crate::tracing::current_context_to_json();
        let submission = Submission {
            id: submission_id,
            prefix: None,
            chunks_total: len,
            chunks_done: ChunkCount::zero(),
            chunk_size,
            metadata,
            otel_trace_carrier,
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
    use crate::{
        common::{
            errors::{DatabaseError, SubmissionNotFound, E},
            StrategicMetadataMap,
        },
        db::{Connection, True, WriterConnection, WriterPool},
    };
    use chunk::ChunkSize;
    use sqlx::{query, query_as, Sqlite};
    use tracing::{info, warn};

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

    #[tracing::instrument(skip(conn))]
    pub async fn insert_submission_raw(
        submission: &Submission,
        mut conn: impl WriterConnection,
    ) -> Result<(), DatabaseError> {
        sqlx::query!(
            "
        INSERT INTO submissions (id, prefix, chunks_total, chunks_done, metadata, otel_trace_carrier, chunk_size)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ",
            submission.id,
            submission.prefix,
            submission.chunks_total,
            submission.chunks_done,
            submission.metadata,
            submission.otel_trace_carrier,
            submission.chunk_size.0,
        )
        .execute(conn.get_inner())
        .await?;

        Ok(())
    }

    pub async fn insert_submission_metadata_raw(
        submission: &Submission,
        strategic_metadata: &StrategicMetadataMap,
        mut conn: impl WriterConnection<Transaction = True>,
    ) -> Result<(), DatabaseError> {
        for (key, value) in strategic_metadata {
            sqlx::query!(
                "
                INSERT INTO submissions_metadata
                ( submission_id
                , metadata_key
                , metadata_value
                )
                VALUES ($1, $2, $3)
                ",
                submission.id,
                key,
                value,
            )
            .execute(conn.get_inner())
            .await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(chunks, conn))]
    pub(crate) async fn insert_submission(
        submission: Submission,
        chunks: Vec<Chunk>,
        strategic_metadata: StrategicMetadataMap,
        mut conn: impl WriterConnection,
    ) -> Result<(), DatabaseError> {
        use axum_prometheus::metrics::counter;
        use futures::FutureExt as _;

        let chunks_total = submission.chunks_total.into();
        tracing::debug!("Inserting submission {}", submission.id);

        let res = conn
            .transaction(move |mut tx| {
                async move {
                    insert_submission_raw(&submission, &mut tx).await?;
                    insert_submission_metadata_raw(&submission, &strategic_metadata, &mut tx)
                        .await?;
                    super::chunk::db::insert_many_chunks(&chunks, &mut tx).await?;
                    super::chunk::db::insert_many_chunks_metadata(
                        &chunks,
                        &strategic_metadata,
                        &mut tx,
                    )
                    .await?;
                    Ok(())
                }
                .boxed()
            })
            .await;

        counter!(crate::prometheus::SUBMISSIONS_TOTAL_COUNTER).increment(1);
        counter!(crate::prometheus::CHUNKS_TOTAL_COUNTER).increment(chunks_total);
        res
    }

    /// Creates a new submission with the given chunks and inserts it into the database.
    ///
    /// If the number of chunks is 0, the submission is marked as completed immediately afterwards.
    #[tracing::instrument(skip(metadata, chunks_contents, conn))]
    pub async fn insert_submission_from_chunks(
        prefix: Option<String>,
        chunks_contents: Vec<chunk::Content>,
        metadata: Option<Metadata>,
        strategic_metadata: StrategicMetadataMap,
        chunk_size: ChunkSize,
        mut conn: impl WriterConnection,
    ) -> Result<SubmissionId, DatabaseError> {
        let submission_id = Submission::generate_id();
        let len = chunks_contents.len().try_into().expect("Vector length larger than u63 range. Unlikely because of RAM constraints but theoretically possible");
        let otel_trace_carrier = crate::tracing::current_context_to_json();
        let submission = Submission {
            id: submission_id,
            prefix,
            chunks_total: len,
            chunks_done: ChunkCount::zero(),
            chunk_size,
            metadata,
            otel_trace_carrier,
        };
        let iter = chunks_contents
            .into_iter()
            .enumerate()
            .map(move |(chunk_index, uri)| {
                // NOTE: Since `len` fits in a u64, these indexes by definition must too!
                Chunk::new(submission_id, chunk_index.try_into().unwrap(), uri)
            })
            .collect();
        insert_submission(submission, iter, strategic_metadata, &mut conn).await?;
        // Empty submissions get special handling: we mark them as completed right away.
        // See https://github.com/channable/opsqueue/issues/86 for rationale.
        if len == 0 {
            match maybe_complete_submission(submission_id, conn).await {
                // Forward our database errors to the caller.
                Err(E::L(e)) => return Err(e),
                // If the submission ID can't be found, that's too bad, but it's not our problem anymore i guess.
                Err(E::R(_)) => warn!(%submission_id, "Presumed zero-length submission not found"),
                // If everything went OK, this *could* still indicate a bug in producer code, so let's just log it.
                // Our future selves might thank us.
                Ok(true) => info!(%submission_id, "Zero-length submission marked as completed"),
                // This should never happen. If it does, better log it.
                Ok(false) => warn!(%submission_id, "Zero-length submission wasn't zero-length?!"),
            }
        }
        Ok(submission_id)
    }

    #[tracing::instrument(skip(conn))]
    pub async fn get_submission(
        id: SubmissionId,
        mut conn: impl Connection,
    ) -> Result<Submission, E<DatabaseError, SubmissionNotFound>> {
        let submission = query_as!(
            Submission,
            r#"
            SELECT id AS "id: SubmissionId"
            , prefix
            , chunks_total AS "chunks_total: ChunkCount"
            , chunks_done AS "chunks_done: ChunkCount"
            , chunk_size AS "chunk_size: ChunkSize"
            , metadata
            , otel_trace_carrier
            FROM submissions WHERE id = $1
            "#,
            id
        )
        .fetch_optional(conn.get_inner())
        .await?;
        match submission {
            None => Err(E::R(SubmissionNotFound(id))),
            Some(submission) => Ok(submission),
        }
    }

    /// Retrieves the earlier stored strategic metadata.
    ///
    /// Primarily for testing and introspection.
    pub async fn get_submission_strategic_metadata(
        id: SubmissionId,
        mut conn: impl Connection,
    ) -> Result<StrategicMetadataMap, DatabaseError> {
        use futures::{future, TryStreamExt};
        let metadata = query!(
            r#"
        SELECT metadata_key, metadata_value FROM submissions_metadata
        WHERE submission_id = $1
        "#,
            id,
        )
        .fetch(conn.get_inner())
        .and_then(|row| future::ok((row.metadata_key, row.metadata_value)))
        .try_collect()
        .await?;
        Ok(metadata)
    }

    #[tracing::instrument(skip(conn))]
    pub async fn lookup_id_by_prefix(
        prefix: &str,
        mut conn: impl Connection,
    ) -> Result<Option<SubmissionId>, DatabaseError> {
        let row = query!(
            r#"
            SELECT id AS "id: SubmissionId" FROM submissions WHERE prefix = $1
            UNION ALL
            SELECT id AS "id: SubmissionId" FROM submissions_completed WHERE prefix = $2
            UNION ALL
            SELECT id AS "id: SubmissionId" FROM submissions_failed WHERE prefix = $3
            "#,
            prefix,
            prefix,
            prefix
        )
        .fetch_optional(conn.get_inner())
        .await?;
        Ok(row.map(|row| row.id))
    }

    #[tracing::instrument(skip(conn))]
    pub async fn submission_status(
        id: SubmissionId,
        mut conn: impl Connection,
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
            , chunk_size AS "chunk_size: ChunkSize"
            , metadata
            , otel_trace_carrier
        FROM submissions WHERE id = $1
        "#,
            id
        )
        .fetch_optional(conn.get_inner())
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
            , chunk_size AS "chunk_size: ChunkSize"
            , metadata
            , completed_at AS "completed_at: DateTime<Utc>"
            , otel_trace_carrier
        FROM submissions_completed WHERE id = $1
        "#,
            id
        )
        .fetch_optional(conn.get_inner())
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
            , chunk_size AS "chunk_size: ChunkSize"
            , metadata
            , failed_at AS "failed_at: DateTime<Utc>"
            , failed_chunk_id AS "failed_chunk_id: ChunkIndex"
            , otel_trace_carrier
        FROM submissions_failed WHERE id = $1
        "#,
            id
        )
        .fetch_optional(conn.get_inner())
        .await?;
        if let Some(failed_submission) = failed_submission {
            let failed_chunk_id = (failed_submission.id, failed_submission.failed_chunk_id).into();
            let failed_chunk = super::chunk::db::get_chunk_failed(failed_chunk_id, conn).await?;
            return Ok(Some(SubmissionStatus::Failed(
                failed_submission,
                failed_chunk,
            )));
        }

        Ok(None)
    }

    #[tracing::instrument(skip(conn))]
    /// Completes the submission, iff all chunks have been completed.
    ///
    /// Returns `true` if all chunks were completed and the submission was marked as completed.
    /// Otherwise, it returns `false`.
    pub async fn maybe_complete_submission(
        id: SubmissionId,
        mut conn: impl WriterConnection,
    ) -> Result<bool, E<DatabaseError, SubmissionNotFound>> {
        conn.transaction(move |mut tx| {
            Box::pin(async move {
                let submission = get_submission(id, &mut tx).await?;

                if submission.chunks_done == submission.chunks_total {
                    complete_submission_raw(id, &mut tx).await?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            })
        })
        .await
    }

    #[tracing::instrument(skip(conn))]
    /// Do not call directly! MUST be called inside a transaction.
    pub(super) async fn complete_submission_raw(
        id: SubmissionId,
        mut conn: impl WriterConnection<Transaction = True>,
    ) -> Result<(), E<DatabaseError, SubmissionNotFound>> {
        let now = chrono::prelude::Utc::now();
        query!(
            "
    SAVEPOINT complete_submission_raw;

    INSERT INTO submissions_completed
    (id, chunks_total, prefix, metadata, completed_at)
    SELECT id, chunks_total, prefix, metadata, julianday($1) FROM submissions WHERE id = $2;

    DELETE FROM submissions WHERE id = $3 RETURNING *;

    RELEASE SAVEPOINT complete_submission_raw;
    ",
            now,
            id,
            id,
        )
        .fetch_one(conn.get_inner())
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

    #[tracing::instrument(skip(conn))]
    pub(super) async fn fail_submission_raw(
        id: SubmissionId,
        failed_chunk_id: ChunkIndex,
        mut conn: impl WriterConnection,
    ) -> sqlx::Result<()> {
        let now = chrono::prelude::Utc::now();

        query!(
            "
    INSERT INTO submissions_failed
    (id, chunks_total, prefix, metadata, failed_at, failed_chunk_id)
    SELECT id, chunks_total, prefix, metadata, julianday($1), $2 FROM submissions WHERE id = $3;

    DELETE FROM submissions WHERE id = $4 RETURNING *;
    ",
            now,
            failed_chunk_id,
            id,
            id,
        )
        .fetch_one(conn.get_inner())
        .await?;
        counter!(crate::prometheus::SUBMISSIONS_FAILED_COUNTER).increment(1);
        histogram!(crate::prometheus::SUBMISSIONS_DURATION_FAIL_HISTOGRAM).record(
            crate::prometheus::time_delta_as_f64(Utc::now() - id.timestamp()),
        );

        Ok(())
    }

    #[tracing::instrument(skip(conn))]
    pub async fn fail_submission(
        id: SubmissionId,
        failed_chunk_index: ChunkIndex,
        failure: String,
        mut conn: impl WriterConnection,
    ) -> sqlx::Result<()> {
        conn.transaction(move |mut tx| {
            Box::pin(
                async move { fail_submission_notx(id, failed_chunk_index, failure, &mut tx).await },
            )
        })
        .await
    }

    /// Do not call directly! Must be called inside a transaction.
    pub async fn fail_submission_notx(
        id: SubmissionId,
        failed_chunk_index: ChunkIndex,
        failure: String,
        mut conn: impl WriterConnection<Transaction = True>,
    ) -> sqlx::Result<()> {
        fail_submission_raw(id, failed_chunk_index, &mut conn).await?;
        super::chunk::db::move_chunk_to_failed_chunks(
            (id, failed_chunk_index).into(),
            failure,
            &mut conn,
        )
        .await?;
        super::chunk::db::skip_remaining_chunks(id, conn).await?;
        Ok(())
    }

    #[tracing::instrument(skip(db))]
    pub async fn count_submissions(mut db: impl Connection) -> sqlx::Result<usize> {
        let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions;")
            .fetch_one(db.get_inner())
            .await?;
        Ok(count.count as usize)
    }

    #[tracing::instrument(skip(db))]
    pub async fn count_submissions_completed(mut db: impl Connection) -> sqlx::Result<usize> {
        let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions_completed;")
            .fetch_one(db.get_inner())
            .await?;
        Ok(count.count as usize)
    }

    #[tracing::instrument(skip(db))]
    pub async fn count_submissions_failed(mut db: impl Connection) -> sqlx::Result<usize> {
        let count = sqlx::query!("SELECT COUNT(1) as count FROM submissions_failed;")
            .fetch_one(db.get_inner())
            .await?;
        Ok(count.count as usize)
    }

    /// Transactionally removes all completed/failed submissions,
    /// including all their chunks and associated strategic metadata.
    ///
    /// Submissions/chunks that are neither failed nor completed are not touched.
    #[tracing::instrument(skip(conn))]
    pub async fn cleanup_old(
        mut conn: impl Connection,
        older_than: DateTime<Utc>,
    ) -> sqlx::Result<()> {
        tracing::info!("Cleaning up old completed/failed submissions...");
        conn.transaction(move |mut tx| {
            Box::pin(async move {
                // Clean up old submissions_metadata
                query!(
                    "DELETE FROM submissions_metadata
                    WHERE submission_id = (
                        SELECT id FROM submissions_completed WHERE completed_at < julianday($1)
                    );",
                    older_than
                )
                .execute(tx.get_inner())
                .await?;
                query!(
                    "DELETE FROM submissions_metadata
                    WHERE submission_id = (
                        SELECT id FROM submissions_failed WHERE failed_at < julianday($1)
                    );",
                    older_than
                )
                .execute(tx.get_inner())
                .await?;

                // Clean up old submissions:
                let n_submissions_completed = query!(
                    "DELETE FROM submissions_completed WHERE completed_at < julianday($1);",
                    older_than
                )
                .execute(tx.get_inner())
                .await?.rows_affected();
                let n_submissions_failed = query!(
                    "DELETE FROM submissions_failed WHERE failed_at < julianday($1);",
                    older_than
                )
                .execute(tx.get_inner())
                .await?.rows_affected();

                let n_chunks_completed = query!(
                    "DELETE FROM chunks_completed WHERE completed_at < julianday($1);",
                    older_than
                )
                .execute(tx.get_inner())
                .await?.rows_affected();
                let n_chunks_failed = query!(
                    "DELETE FROM chunks_failed WHERE failed_at < julianday($1);",
                    older_than
                )
                .execute(tx.get_inner())
                .await?.rows_affected();

                tracing::info!("Deleted {n_submissions_completed} completed submissions (with {n_chunks_completed} chunks)");
                tracing::info!("Deleted {n_submissions_failed} failed submissions (with {n_chunks_failed} chunks)");
                Ok(())
            })
        })
        .await
    }

    pub async fn periodically_cleanup_old(db: &WriterPool, max_age: Duration) {
        const PERIODIC_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
        loop {
            let cutoff = Utc::now() - max_age;
            let res: sqlx::Result<()> = async move {
                let mut conn = db.writer_conn().await?;
                cleanup_old(&mut conn, cutoff).await?;
                Ok(())
            }
            .await;
            if let Err(e) = res {
                tracing::error!("Error during periodic cleanup: {}", e);
            }
            tokio::time::sleep(PERIODIC_CLEANUP_INTERVAL).await;
        }
    }
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {

    use assert_matches::*;
    use chrono::Utc;
    use chunk::ChunkSize;

    use crate::common::StrategicMetadataMap;
    use crate::db::{Connection as _, WriterPool};

    use super::db::*;
    use super::*;

    #[sqlx::test]
    pub async fn test_insert_submission(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();

        assert!(count_submissions(&mut conn).await.unwrap() == 0);

        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
            ChunkSize::default(),
        )
        .unwrap();
        insert_submission(submission, chunks, Default::default(), &mut conn)
            .await
            .expect("insertion failed");

        assert_matches!(count_submissions(&mut conn).await, Ok(1));
    }

    #[sqlx::test]
    pub async fn test_get_submission(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();
        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
            ChunkSize(1),
        )
        .unwrap();
        insert_submission(submission.clone(), chunks, Default::default(), &mut conn)
            .await
            .expect("insertion failed");

        let fetched_submission = get_submission(submission.id, &mut conn).await.unwrap();
        assert_eq!(fetched_submission, submission);
    }

    #[sqlx::test]
    pub async fn test_submission_strategic_metadata(db: sqlx::SqlitePool) {
        let strategic_metadata: StrategicMetadataMap =
            [("company_id".to_string(), 123), ("flavour".to_string(), 42)]
                .into_iter()
                .collect();
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();
        let chunks = vec![Some("foo".into()), Some("bar".into()), Some("baz".into())];

        let submission_id = insert_submission_from_chunks(
            None,
            chunks,
            None,
            strategic_metadata.clone(),
            ChunkSize::default(),
            &mut conn,
        )
        .await
        .expect("insertion failed");

        let fetched_metadata = get_submission_strategic_metadata(submission_id, &mut conn)
            .await
            .unwrap();
        assert!(fetched_metadata == strategic_metadata);

        let res = sqlx::query!("SELECT * FROM chunks_metadata;")
            .fetch_all(conn.get_inner())
            .await
            .unwrap();
        dbg!(res);

        let chunk_id = (submission_id, ChunkIndex::zero()).into();
        let chunk_fetched_metadata = chunk::db::get_chunk_strategic_metadata(chunk_id, &mut conn)
            .await
            .unwrap();

        dbg!(&strategic_metadata);
        dbg!(&fetched_metadata);
        dbg!(&chunk_fetched_metadata);

        assert_eq!(chunk_fetched_metadata, strategic_metadata);
    }

    #[sqlx::test]
    pub async fn test_complete_submission_raw(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();
        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
            ChunkSize::default(),
        )
        .unwrap();
        insert_submission(submission.clone(), chunks, Default::default(), &mut conn)
            .await
            .expect("insertion failed");

        conn.transaction(move |mut tx| {
            Box::pin(async move { complete_submission_raw(submission.id, &mut tx).await })
        })
        .await
        .unwrap();

        assert_matches!(count_submissions(&mut conn).await, Ok(0));
        assert_matches!(count_submissions_completed(&mut conn).await, Ok(1));
        assert_matches!(count_submissions_failed(&mut conn).await, Ok(0));
    }

    #[sqlx::test]
    pub async fn test_fail_submission_raw(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();
        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
            ChunkSize::default(),
        )
        .unwrap();
        insert_submission(submission.clone(), chunks, Default::default(), &mut conn)
            .await
            .expect("insertion failed");

        fail_submission(
            submission.id,
            u63::new(1).into(),
            "Boom!".to_string(),
            &mut conn,
        )
        .await
        .unwrap();
        assert_matches!(count_submissions(&mut conn).await, Ok(0));
        assert_matches!(count_submissions_completed(&mut conn).await, Ok(0));
        assert_matches!(count_submissions_failed(&mut conn).await, Ok(1));
    }

    #[sqlx::test]
    pub async fn test_cleanup_old(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();

        let chunks_contents = vec![Some("foo".into()), Some("bar".into()), Some("baz".into())];
        let old_one = insert_submission_from_chunks(
            None,
            chunks_contents.clone(),
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            &mut conn,
        )
        .await
        .unwrap();
        let old_two = insert_submission_from_chunks(
            None,
            chunks_contents.clone(),
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            &mut conn,
        )
        .await
        .unwrap();
        let old_three = insert_submission_from_chunks(
            None,
            chunks_contents.clone(),
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            &mut conn,
        )
        .await
        .unwrap();
        let old_four_unfailed = insert_submission_from_chunks(
            None,
            chunks_contents.clone(),
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            &mut conn,
        )
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

        // Ensure the clock is advanced ever so slightly.
        // Not doing this makes the test flaky.
        tokio::time::sleep(Duration::from_millis(1)).await;

        let cutoff_timestamp = Utc::now();

        let too_new_one = insert_submission_from_chunks(
            None,
            chunks_contents.clone(),
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            &mut conn,
        )
        .await
        .unwrap();
        let _too_new_two_unfailed = insert_submission_from_chunks(
            None,
            chunks_contents.clone(),
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            &mut conn,
        )
        .await
        .unwrap();
        let too_new_three = insert_submission_from_chunks(
            None,
            chunks_contents.clone(),
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            &mut conn,
        )
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

        assert_matches!(count_submissions_failed(&mut conn).await, Ok(5));

        let mut conn2 = db.writer_conn().await.unwrap();
        cleanup_old(&mut conn2, cutoff_timestamp).await.unwrap();

        assert_matches!(count_submissions_failed(&mut conn).await, Ok(2));

        let _sub1 = submission_status(old_four_unfailed, &mut conn).await;
        let _sub2 = submission_status(old_four_unfailed, &mut conn).await;
    }

    #[sqlx::test]
    /// Test whether empty submissions are marked as completed right away by `insert_submission_from_chunks`.
    pub async fn auto_complete_empty_submission(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();
        insert_submission_from_chunks(
            // prefix
            None,
            // chunks
            vec![],
            // metadata
            None,
            // strategic_metadata
            StrategicMetadataMap::default(),
            // chunk size
            ChunkSize::default(),
            &mut conn,
        )
        .await
        .expect("insertion failed");

        assert_matches!(count_submissions(&mut conn).await, Ok(0));
        assert_matches!(count_submissions_completed(&mut conn).await, Ok(1));
        assert_matches!(count_submissions_failed(&mut conn).await, Ok(0));
    }
}

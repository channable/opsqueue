//! Dealing with `Submission`s: Collections of (`Chunks`s of) operations.
use std::fmt::Display;
use std::time::Duration;

#[cfg(feature = "server-logic")]
use crate::E;
use crate::common::StrategicMetadataMap;
use chrono::{DateTime, Utc};
use ux::u63;

use super::chunk::{self, Chunk, ChunkFailed, ChunkSize};
use super::chunk::{ChunkCount, ChunkIndex};

pub type Metadata = Vec<u8>;

static ID_GENERATOR: snowflaked::sync::Generator = snowflaked::sync::Generator::new(0);

/// Uniquely identifies a [`Submission`].
///
/// Submission IDs are snowflakes. These are 64-bit identifiers that includes
/// creation time information and are sortable on that timestamp.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubmissionId(u63);

impl serde::Serialize for SubmissionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        u64::from(*self).serialize(serializer)
    }
}

impl<'a> serde::Deserialize<'a> for SubmissionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let value = u64::deserialize(deserializer)?;

        value.try_into().map_err(serde::de::Error::custom)
    }
}

impl Default for SubmissionId {
    fn default() -> Self {
        Self::new()
    }
}

impl SubmissionId {
    /// Generate a new [`SubmissionId`].
    pub fn new() -> SubmissionId {
        let inner: u64 = ID_GENERATOR.generate();
        SubmissionId(u63::new(inner))
    }
    /// Access the [`std::time::SystemTime`] at which the ID was generated.
    ///
    /// # Panics
    ///
    /// Panics if the timestamp encoded in the snowflake id cannot be represented
    /// relative to the configured epoch.
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
    /// Get the time at which the submission ID was generated as a [`chrono::DateTime`].
    #[must_use]
    pub fn timestamp(self) -> chrono::DateTime<Utc> {
        self.system_time().into()
    }
}

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
        inner.cast_signed()
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

impl TryFrom<i64> for SubmissionId {
    type Error = crate::common::errors::TryFromIntError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value < 0 {
            return Err(crate::common::errors::TryFromIntError(()));
        }

        Ok(Self(u63::new(value.cast_unsigned())))
    }
}

impl From<&SubmissionId> for std::time::SystemTime {
    fn from(val: &SubmissionId) -> Self {
        val.system_time()
    }
}

/// A submission is a group of [`Chunk`]s.
///
/// This struct represents a submission that has not yet reached an end state.
/// That is, they are neither completed nor failed.
///
/// Also see [`SubmissionCompleted`] and [`SubmissionFailed`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Submission {
    pub id: SubmissionId,
    pub prefix: Option<String>,
    pub chunks_total: ChunkCount,
    pub chunks_done: ChunkCount,
    pub chunk_size: ChunkSize,
    pub metadata: Option<Metadata>,
    #[serde(default)]
    pub strategic_metadata: StrategicMetadataMap,
    pub otel_trace_carrier: String,
}

/// A submission that has been completed successfully.
///
/// Once a submission is marked completed (which happens when all its chunks are
/// marked as done), it gets moved to the `submissions_completed` table, and its
/// old `submissions` record gets deleted.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionCompleted {
    pub id: SubmissionId,
    pub prefix: Option<String>,
    pub chunks_total: ChunkCount,
    pub chunk_size: ChunkSize,
    pub metadata: Option<Metadata>,
    #[serde(default)]
    pub strategic_metadata: StrategicMetadataMap,
    pub completed_at: DateTime<Utc>,
    pub otel_trace_carrier: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionFailed {
    pub id: SubmissionId,
    pub prefix: Option<String>,
    pub chunks_total: ChunkCount,
    pub chunks_done: Option<ChunkCount>,
    pub chunk_size: ChunkSize,
    pub metadata: Option<Metadata>,
    #[serde(default)]
    pub strategic_metadata: StrategicMetadataMap,
    pub failed_at: DateTime<Utc>,
    pub failed_chunk_id: ChunkIndex,
    pub otel_trace_carrier: String,
}

/// A submission that has been cancelled.
///
/// Once a submission is cancelled, it gets moved to the `submissions_cancelled`
/// table, and its old `submissions` record gets deleted.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionCancelled {
    pub id: SubmissionId,
    pub prefix: Option<String>,
    pub chunks_total: ChunkCount,
    pub chunks_done: ChunkCount,
    pub metadata: Option<Metadata>,
    #[serde(default)]
    pub strategic_metadata: StrategicMetadataMap,
    pub cancelled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubmissionStatus {
    InProgress(Submission),
    Completed(SubmissionCompleted),
    Failed(SubmissionFailed, ChunkFailed),
    Cancelled(SubmissionCancelled),
}

impl Default for Submission {
    fn default() -> Self {
        Self::new()
    }
}

impl Submission {
    #[must_use]
    pub fn new() -> Self {
        let otel_trace_carrier = crate::tracing::current_context_to_json();
        Submission {
            id: SubmissionId(u63::new(0)),
            prefix: None,
            chunks_total: ChunkCount::zero(),
            chunks_done: ChunkCount::zero(),
            chunk_size: ChunkSize::default(),
            metadata: None,
            strategic_metadata: StrategicMetadataMap::default(),
            otel_trace_carrier,
        }
    }

    #[must_use]
    pub fn from_vec(
        chunks: Vec<chunk::Content>,
        metadata: Option<Metadata>,
        chunk_size: ChunkSize,
    ) -> Option<(Submission, Vec<Chunk>)> {
        let submission_id = SubmissionId::new();
        let len = ChunkCount::new(u64::try_from(chunks.len()).ok()?).ok()?;
        let otel_trace_carrier = crate::tracing::current_context_to_json();
        let submission = Submission {
            id: submission_id,
            prefix: None,
            chunks_total: len,
            chunks_done: ChunkCount::zero(),
            chunk_size,
            metadata,
            strategic_metadata: StrategicMetadataMap::default(),
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
            MaxSubmissions, StrategicMetadataMap,
            errors::{
                DatabaseError, E, SubmissionNotCancellable, SubmissionNotFound,
                TooManyMatchingSubmissions,
            },
        },
        db::{Connection, True, WriterConnection, WriterPool},
    };
    use axum_prometheus::metrics::{counter, histogram};
    use chunk::ChunkSize;
    use sqlx::{QueryBuilder, Sqlite, query, query_scalar};
    use ux::u63;

    use super::{
        Chunk, ChunkCount, ChunkIndex, DateTime, Duration, E, Metadata, Submission,
        SubmissionCancelled, SubmissionCompleted, SubmissionFailed, SubmissionId, SubmissionStatus,
        Utc, chunk,
    };

    impl<'q> sqlx::Encode<'q, Sqlite> for SubmissionId {
        fn encode_by_ref(
            &self,
            buf: &mut <Sqlite as sqlx::Database>::ArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            <i64 as sqlx::Encode<'q, Sqlite>>::encode_by_ref(&i64::from(*self), buf)
        }

        fn encode(
            self,
            buf: &mut <Sqlite as sqlx::Database>::ArgumentBuffer,
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
    /// Insert the submission record into the database.
    ///
    /// # Errors
    ///
    /// Returns an error if insertion fails.
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

    /// Insert strategic metadata rows for a submission.
    ///
    /// # Errors
    ///
    /// Returns an error if insertion of any metadata row fails.
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
                    insert_submission_metadata_raw(
                        &submission,
                        &submission.strategic_metadata,
                        &mut tx,
                    )
                    .await?;
                    super::chunk::db::insert_many_chunks(&chunks, &mut tx).await?;
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
    ///
    /// # Panics
    ///
    /// Panics if the chunk vector length cannot be represented as `ChunkCount`.
    ///
    /// # Errors
    ///
    /// Returns an error if insertion or follow-up completion updates fail.
    #[tracing::instrument(skip(metadata, chunks_contents, conn))]
    pub async fn insert_submission_from_chunks(
        prefix: Option<String>,
        chunks_contents: Vec<chunk::Content>,
        metadata: Option<Metadata>,
        strategic_metadata: StrategicMetadataMap,
        chunk_size: ChunkSize,
        mut conn: impl WriterConnection,
    ) -> Result<SubmissionId, DatabaseError> {
        let submission_id = SubmissionId::new();
        let len = chunks_contents.len().try_into().expect("Vector length larger than u63 range. Unlikely because of RAM constraints but theoretically possible");
        let otel_trace_carrier = crate::tracing::current_context_to_json();
        let submission = Submission {
            id: submission_id,
            prefix,
            chunks_total: len,
            chunks_done: ChunkCount::zero(),
            chunk_size,
            metadata,
            strategic_metadata,
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
        insert_submission(submission, iter, &mut conn).await?;
        // Empty submissions get special handling: we mark them as completed right away.
        // See https://github.com/channable/opsqueue/issues/86 for rationale.
        if len == 0 {
            match maybe_complete_submission(submission_id, conn).await {
                // Forward our database errors to the caller.
                Err(E::L(e)) => return Err(e),
                // If the submission ID can't be found, that's too bad, but it's not our problem anymore i guess.
                Err(E::R(_)) => {
                    tracing::warn!(%submission_id, "Presumed zero-length submission not found");
                }
                // If everything went OK, this *could* still indicate a bug in producer code, so let's just log it.
                // Our future selves might thank us.
                Ok(true) => {
                    tracing::debug!(%submission_id, "Zero-length submission marked as completed");
                }
                // This should never happen. If it does, better log it.
                Ok(false) => {
                    tracing::warn!(%submission_id, "Zero-length submission wasn't zero-length?!");
                }
            }
        }
        Ok(submission_id)
    }

    /// Fetch an in-progress submission by id.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or if the submission is not found.
    #[tracing::instrument(skip(conn))]
    pub async fn get_submission(
        id: SubmissionId,
        mut conn: impl Connection,
    ) -> Result<Submission, E<DatabaseError, SubmissionNotFound>> {
        let submission_row = query!(
            r#"
            SELECT id AS "id: SubmissionId"
            , prefix
            , chunks_total AS "chunks_total: ChunkCount"
            , chunks_done AS "chunks_done: ChunkCount"
            , chunk_size AS "chunk_size!: ChunkSize"
            , metadata
            , ( SELECT json_group_object(metadata_key, metadata_value)
                FROM submissions_metadata
                WHERE submission_id = submissions.id
              ) AS "strategic_metadata!: sqlx::types::Json<StrategicMetadataMap>"
            , otel_trace_carrier
            FROM submissions WHERE id = $1
            "#,
            id
        )
        .fetch_optional(conn.get_inner())
        .await?;
        match submission_row {
            None => Err(E::R(SubmissionNotFound(id))),
            Some(row) => Ok(Submission {
                id: row.id,
                prefix: row.prefix,
                chunks_total: row.chunks_total,
                chunks_done: row.chunks_done,
                chunk_size: row.chunk_size,
                metadata: row.metadata,
                strategic_metadata: row.strategic_metadata.0,
                otel_trace_carrier: row.otel_trace_carrier,
            }),
        }
    }

    /// Retrieves the earlier stored strategic metadata.
    ///
    /// Primarily for testing and introspection.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_submission_strategic_metadata(
        id: SubmissionId,
        mut conn: impl Connection,
    ) -> Result<StrategicMetadataMap, DatabaseError> {
        use futures::{TryStreamExt, future};
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

    /// Look up a submission id by prefix across submission states.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
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

    /// Look up submission ids matching strategic metadata constraints.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or too many submissions match.
    pub async fn lookup_ids_by_strategic_metadata(
        strategic_metadata: StrategicMetadataMap,
        max_submissions: MaxSubmissions,
        mut conn: impl Connection,
    ) -> Result<Vec<SubmissionId>, E<DatabaseError, TooManyMatchingSubmissions>> {
        // MaxSubmissions provides us with the guarantee this won't overflow.
        let limit = (u64::from(max_submissions) + 1).cast_signed();
        // The main query to match on strategic_metadata will fail at run-time
        // if strategic_metadata is empty, so we handle the empty case here.
        let ids = if strategic_metadata.is_empty() {
            query_scalar!(
                r#"SELECT id AS "id: SubmissionId" FROM submissions ORDER BY id LIMIT ?"#,
                limit
            )
            .fetch_all(conn.get_inner())
            .await?
        } else {
            let mut query_builder: QueryBuilder<Sqlite> =
                lookup_ids_by_strategic_metadata_query(&strategic_metadata, limit);
            query_builder
                .build_query_scalar()
                .fetch_all(conn.get_inner())
                .await?
        };
        if ids.len() as u64 > u64::from(max_submissions) {
            Err(E::R(TooManyMatchingSubmissions(u64::from(max_submissions))))
        } else {
            Ok(ids)
        }
    }

    /// The query in '`lookup_ids_by_strategic_metadata`', extracted for testing.
    #[must_use]
    pub fn lookup_ids_by_strategic_metadata_query(
        strategic_metadata: &StrategicMetadataMap,
        limit: i64,
    ) -> QueryBuilder<Sqlite> {
        let mut query_builder: QueryBuilder<Sqlite> =
            QueryBuilder::new("SELECT id FROM submissions");
        // Inner join for each piece of strategic metadata.
        for (i, (key, value)) in strategic_metadata.iter().enumerate() {
            query_builder.push(format!(
                " INNER JOIN submissions_metadata AS s{i} ON s{i}.submission_id = submissions.id"
            ));
            query_builder.push(format!(" AND s{i}.metadata_key = "));
            query_builder.push_bind(key.clone());
            query_builder.push(format!(" AND s{i}.metadata_value = "));
            query_builder.push_bind(*value);
        }
        query_builder.push(" ORDER BY s0.submission_id LIMIT ");
        query_builder.push_bind(limit);
        query_builder
    }

    /// Resolve the status of a submission id across all submission tables.
    ///
    /// # Errors
    ///
    /// Returns an error if one of the underlying queries fails.
    #[tracing::instrument(skip(conn))]
    #[allow(clippy::too_many_lines)]
    pub async fn submission_status(
        id: SubmissionId,
        mut conn: impl Connection,
    ) -> Result<Option<SubmissionStatus>, DatabaseError> {
        // NOTE: The order is important here; a concurrent writer could move a submission
        // from InProgress to Completed/Failed in-between the queries.

        let submission_row = query!(
            r#"
        SELECT
              id AS "id: SubmissionId"
            , prefix
            , chunks_total AS "chunks_total: ChunkCount"
            , chunks_done AS "chunks_done: ChunkCount"
            , chunk_size AS "chunk_size!: ChunkSize"
            , metadata
            , ( SELECT json_group_object(metadata_key, metadata_value)
                FROM submissions_metadata
                WHERE submission_id = submissions.id
              ) AS "strategic_metadata!: sqlx::types::Json<StrategicMetadataMap>"
            , otel_trace_carrier
        FROM submissions WHERE id = $1
        "#,
            id
        )
        .fetch_optional(conn.get_inner())
        .await?;
        if let Some(row) = submission_row {
            let submission = Submission {
                id: row.id,
                prefix: row.prefix,
                chunks_total: row.chunks_total,
                chunks_done: row.chunks_done,
                chunk_size: row.chunk_size,
                metadata: row.metadata,
                strategic_metadata: row.strategic_metadata.0,
                otel_trace_carrier: row.otel_trace_carrier,
            };
            return Ok(Some(SubmissionStatus::InProgress(submission)));
        }

        let completed_row_opt = query!(
            r#"
        SELECT
            id AS "id: SubmissionId"
            , prefix
            , chunks_total AS "chunks_total: ChunkCount"
            , chunk_size AS "chunk_size!: ChunkSize"
            , metadata
            , ( SELECT json_group_object(metadata_key, metadata_value)
                FROM submissions_metadata
                WHERE submission_id = submissions_completed.id
              ) AS "strategic_metadata!: sqlx::types::Json<StrategicMetadataMap>"
            , completed_at AS "completed_at: DateTime<Utc>"
            , otel_trace_carrier
        FROM submissions_completed WHERE id = $1
        "#,
            id
        )
        .fetch_optional(conn.get_inner())
        .await?;
        if let Some(row) = completed_row_opt {
            let submission_completed = SubmissionCompleted {
                id: row.id,
                prefix: row.prefix,
                chunks_total: row.chunks_total,
                chunk_size: row.chunk_size,
                metadata: row.metadata,
                strategic_metadata: row.strategic_metadata.0,
                completed_at: row.completed_at,
                otel_trace_carrier: row.otel_trace_carrier,
            };
            return Ok(Some(SubmissionStatus::Completed(submission_completed)));
        }

        let failed_row_opt = query!(
            r#"
        SELECT
              id AS "id: SubmissionId"
            , prefix
            , chunks_total AS "chunks_total: ChunkCount"
            , chunks_done AS "chunks_done: ChunkCount"
            , chunk_size AS "chunk_size!: ChunkSize"
            , metadata
            , ( SELECT json_group_object(metadata_key, metadata_value)
                FROM submissions_metadata
                WHERE submission_id = submissions_failed.id
              ) AS "strategic_metadata!: sqlx::types::Json<StrategicMetadataMap>"
            , failed_at AS "failed_at: DateTime<Utc>"
            , failed_chunk_id AS "failed_chunk_id: ChunkIndex"
            , otel_trace_carrier
        FROM submissions_failed WHERE id = $1
        "#,
            id
        )
        .fetch_optional(conn.get_inner())
        .await?;
        if let Some(row) = failed_row_opt {
            let failed_submission = SubmissionFailed {
                id: row.id,
                prefix: row.prefix,
                chunks_total: row.chunks_total,
                chunks_done: row.chunks_done,
                chunk_size: row.chunk_size,
                metadata: row.metadata,
                strategic_metadata: row.strategic_metadata.0,
                failed_at: row.failed_at,
                failed_chunk_id: row.failed_chunk_id,
                otel_trace_carrier: row.otel_trace_carrier,
            };
            let failed_chunk_id = (row.id, row.failed_chunk_id).into();
            let failed_chunk = super::chunk::db::get_chunk_failed(failed_chunk_id, conn).await?;
            return Ok(Some(SubmissionStatus::Failed(
                failed_submission,
                failed_chunk,
            )));
        }

        let cancelled_row_opt = query!(
            r#"
        SELECT
              id AS "id: SubmissionId"
            , prefix
            , chunks_total AS "chunks_total: ChunkCount"
            , chunks_done AS "chunks_done: ChunkCount"
            , metadata
            , ( SELECT json_group_object(metadata_key, metadata_value)
                FROM submissions_metadata
                WHERE submission_id = submissions_cancelled.id
              ) AS "strategic_metadata!: sqlx::types::Json<StrategicMetadataMap>"
            , cancelled_at AS "cancelled_at: DateTime<Utc>"
        FROM submissions_cancelled WHERE id = $1
        "#,
            id
        )
        .fetch_optional(conn.get_inner())
        .await?;
        if let Some(row) = cancelled_row_opt {
            let cancelled_submission = SubmissionCancelled {
                id: row.id,
                prefix: row.prefix,
                chunks_total: row.chunks_total,
                chunks_done: row.chunks_done,
                metadata: row.metadata,
                strategic_metadata: row.strategic_metadata.0,
                cancelled_at: row.cancelled_at,
            };
            return Ok(Some(SubmissionStatus::Cancelled(cancelled_submission)));
        }

        Ok(None)
    }

    #[tracing::instrument(skip(conn))]
    /// Completes the submission, iff all chunks have been completed.
    ///
    /// Returns `true` if all chunks were completed and the submission was marked as completed.
    /// Otherwise, it returns `false`.
    ///
    /// # Errors
    ///
    /// Returns an error if the submission cannot be loaded or updated.
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

    /// Cancel a submission if it is still cancellable.
    ///
    /// # Panics
    ///
    /// Panics if internal state becomes inconsistent and an in-progress submission
    /// cannot be cancelled even though it should still exist.
    ///
    /// # Errors
    ///
    /// Returns an error if database operations fail, the submission is missing,
    /// or it is already in a terminal non-cancellable state.
    #[tracing::instrument(skip(conn))]
    pub async fn cancel_submission(
        id: SubmissionId,
        mut conn: impl WriterConnection,
    ) -> Result<(), E![DatabaseError, SubmissionNotFound, SubmissionNotCancellable]> {
        conn.transaction(move |mut tx| {
            Box::pin(async move {
                match cancel_submission_notx(id, &mut tx).await {
                    Ok(()) => Ok(()),
                    Err(E::L(db_err)) => Err(E::L(db_err)),
                    Err(E::R(not_found_err)) => {
                        // Submission was not found in the 'submissions' table,
                        // but it could still be in one of the other tables.
                        match submission_status(id, &mut tx).await {
                            Ok(None) => Err(E::R(E::L(not_found_err))),
                            Ok(Some(SubmissionStatus::InProgress(submission))) => {
                                panic!("Failed to cancel in progress submission {submission:?}")
                            }
                            Ok(Some(SubmissionStatus::Completed(submission))) => {
                                Err(E::R(E::R(SubmissionNotCancellable::Completed(submission))))
                            }
                            Ok(Some(SubmissionStatus::Failed(submission, chunk))) => Err(E::R(
                                E::R(SubmissionNotCancellable::Failed(submission, chunk)),
                            )),
                            Ok(Some(SubmissionStatus::Cancelled(submission))) => {
                                Err(E::R(E::R(SubmissionNotCancellable::Cancelled(submission))))
                            }
                            Err(db_err) => Err(E::L(db_err)),
                        }
                    }
                }
            })
        })
        .await
    }

    /// Do not call directly! Must be called inside a transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if cancellation or chunk skipping fails.
    pub async fn cancel_submission_notx(
        id: SubmissionId,
        mut conn: impl WriterConnection<Transaction = True>,
    ) -> Result<(), E<DatabaseError, SubmissionNotFound>> {
        cancel_submission_raw(id, &mut conn).await?;
        super::chunk::db::skip_remaining_chunks(id, conn).await?;
        Ok(())
    }

    #[tracing::instrument(skip(conn))]
    pub(super) async fn cancel_submission_raw(
        id: SubmissionId,
        mut conn: impl WriterConnection,
    ) -> Result<(), E<DatabaseError, SubmissionNotFound>> {
        let now = chrono::prelude::Utc::now();

        let submission_opt = query!(
            "
    INSERT INTO submissions_cancelled
    (id, chunks_total, prefix, metadata, cancelled_at, chunks_done)
    SELECT id, chunks_total, prefix, metadata, julianday($1), chunks_done FROM submissions WHERE id = $2;

    DELETE FROM submissions WHERE id = $3 RETURNING *;
    ",
            now,
            id,
            id,
        )
        .fetch_optional(conn.get_inner())
        .await?;
        if submission_opt.is_none() {
            Err(E::R(SubmissionNotFound(id)))
        } else {
            counter!(crate::prometheus::SUBMISSIONS_CANCELLED_COUNTER).increment(1);
            histogram!(crate::prometheus::SUBMISSIONS_DURATION_CANCEL_HISTOGRAM).record(
                crate::prometheus::time_delta_as_f64(Utc::now() - id.timestamp()),
            );
            Ok(())
        }
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
    (id, chunks_total, chunks_done, prefix, metadata, failed_at, failed_chunk_id)
    SELECT id, chunks_total, chunks_done, prefix, metadata, julianday($1), $2 FROM submissions WHERE id = $3;

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

    /// Fail a submission and move related chunk state in one transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the transactional updates fail.
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
    ///
    /// # Errors
    ///
    /// Returns an error if submission/chunk failure transitions cannot be persisted.
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

    /// Count in-progress submissions.
    ///
    /// # Errors
    ///
    /// Returns an error if the count query fails.
    #[tracing::instrument(skip(db))]
    pub async fn count_submissions(mut db: impl Connection) -> sqlx::Result<u63> {
        let count = sqlx::query_scalar!("SELECT COUNT(1) as count FROM submissions;")
            .fetch_one(db.get_inner())
            .await?;
        Ok(u63::new(count.cast_unsigned()))
    }

    /// Count completed submissions.
    ///
    /// # Errors
    ///
    /// Returns an error if the count query fails.
    #[tracing::instrument(skip(db))]
    pub async fn count_submissions_completed(mut db: impl Connection) -> sqlx::Result<u63> {
        let count = sqlx::query_scalar!("SELECT COUNT(1) as count FROM submissions_completed;")
            .fetch_one(db.get_inner())
            .await?;
        Ok(u63::new(count.cast_unsigned()))
    }

    /// Count failed submissions.
    ///
    /// # Errors
    ///
    /// Returns an error if the count query fails.
    #[tracing::instrument(skip(db))]
    pub async fn count_submissions_failed(mut db: impl Connection) -> sqlx::Result<u63> {
        let count = sqlx::query_scalar!("SELECT COUNT(1) as count FROM submissions_failed;")
            .fetch_one(db.get_inner())
            .await?;
        Ok(u63::new(count.cast_unsigned()))
    }

    /// Transactionally removes all completed/failed submissions,
    /// including all their chunks and associated strategic metadata.
    ///
    /// Submissions/chunks that are neither failed nor completed are not touched.
    ///
    /// # Errors
    ///
    /// Returns an error if any cleanup statement in the transaction fails.
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
                    WHERE submission_id IN (
                        SELECT id FROM submissions_completed WHERE completed_at < julianday($1)
                    );",
                    older_than
                )
                .execute(tx.get_inner())
                .await?;
                query!(
                    "DELETE FROM submissions_metadata
                    WHERE submission_id IN (
                        SELECT id FROM submissions_failed WHERE failed_at < julianday($1)
                    );",
                    older_than
                )
                .execute(tx.get_inner())
                .await?;
                query!(
                    "DELETE FROM submissions_metadata
                    WHERE submission_id IN (
                        SELECT id FROM submissions_cancelled WHERE cancelled_at < julianday($1)
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
                let n_submissions_cancelled = query!(
                    "DELETE FROM submissions_cancelled WHERE cancelled_at < julianday($1);",
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

                tracing::info!("Deleted {n_submissions_completed} completed submissions (with {n_chunks_completed} chunks completed)");
                tracing::info!("Deleted {n_submissions_failed} failed submissions (with {n_chunks_failed} chunks failed)");
                tracing::info!("Deleted {n_submissions_cancelled} cancelled submissions");
                Ok(())
            })
        })
        .await
    }

    pub async fn periodically_cleanup_old(db: &WriterPool, max_age: Duration) {
        const PERIODIC_CLEANUP_INTERVAL: Duration = Duration::from_mins(1);
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
    use chrono::Utc;
    use chunk::ChunkSize;
    use itertools::Itertools;
    use sqlformat::{FormatOptions, QueryParams, format};
    use sqlx::{Row, SqliteConnection};

    use crate::common::StrategicMetadataMap;
    use crate::db::{Connection as _, WriterPool};

    use super::db::*;
    use super::*;

    async fn explain_query_plan(query: &str, conn: &mut SqliteConnection) -> String {
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!("EXPLAIN QUERY PLAN {query}")))
            .fetch_all(&mut *conn)
            .await
            .unwrap_or_else(|_| panic!("Invalid query: \n{query}\n"))
            .into_iter()
            .map(|row| {
                let id = row.get::<i64, &str>("id");
                let parent = row.get::<i64, &str>("parent");
                let detail = row.get::<String, &str>("detail");
                format!("{id}, {parent}, {detail}")
            })
            .join("\n")
    }

    fn assert_non_regressing_query_plan(query: &str, explained: &str) {
        assert!(
            !explained.contains("MATERIALIZED"),
            "Query should contain no materialization, but it did.\n\nQuery: {query}\n\nPlan:\n\n{explained}"
        );
        assert!(
            !explained.contains("B-TREE"),
            "Query should contain no temporary B-tree construction, but it did.\n\nQuery: {query}\n\nPlan:\n\n{explained}"
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_lookup_by_strategic_metadata(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let strategic_metadata: StrategicMetadataMap =
            [("company_id".to_string(), 1), ("project_id".to_string(), 2)]
                .into_iter()
                .collect();
        let qb = lookup_ids_by_strategic_metadata_query(&strategic_metadata, 100_000);
        let options = FormatOptions::default();
        let formatted_query = format(qb.sql().as_str(), &QueryParams::None, &options);
        insta::assert_snapshot!(formatted_query, @"
        SELECT
          id
        FROM
          submissions
          INNER JOIN submissions_metadata AS s0 ON s0.submission_id = submissions.id
          AND s0.metadata_key = ?
          AND s0.metadata_value = ?
          INNER JOIN submissions_metadata AS s1 ON s1.submission_id = submissions.id
          AND s1.metadata_key = ?
          AND s1.metadata_value = ?
        ORDER BY
          s0.submission_id
        LIMIT
          ?
        ");
        let explained = explain_query_plan(&formatted_query, &mut conn).await;
        assert_non_regressing_query_plan(&formatted_query, &explained);
        insta::assert_snapshot!(explained, @"
        8, 0, SEARCH s0 USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=?)
        16, 0, SEARCH submissions USING COVERING INDEX sqlite_autoindex_submissions_1 (id=?)
        21, 0, SEARCH s1 USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_submission_status_in_progress(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let query = r"
        SELECT
              id
            , prefix
            , chunks_total
            , chunks_done
            , chunk_size
            , metadata
            , ( SELECT json_group_object(metadata_key, metadata_value)
                FROM submissions_metadata
                WHERE submission_id = submissions.id
              ) AS strategic_metadata
            , otel_trace_carrier
        FROM submissions WHERE id = 1
        ";

        let explained = explain_query_plan(query, &mut conn).await;
        assert_non_regressing_query_plan(query, &explained);
        insta::assert_snapshot!(explained, @r"
        3, 0, SEARCH submissions USING INDEX sqlite_autoindex_submissions_1 (id=?)
        15, 0, CORRELATED SCALAR SUBQUERY 1
        20, 15, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_submission_status_completed(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let query = r"
        SELECT
              id
            , prefix
            , chunks_total
            , chunk_size
            , metadata
            , ( SELECT json_group_object(metadata_key, metadata_value)
                FROM submissions_metadata
                WHERE submission_id = submissions_completed.id
              ) AS strategic_metadata
            , completed_at
            , otel_trace_carrier
        FROM submissions_completed WHERE id = 1
        ";

        let explained = explain_query_plan(query, &mut conn).await;
        assert_non_regressing_query_plan(query, &explained);
        insta::assert_snapshot!(explained, @r"
        3, 0, SEARCH submissions_completed USING INDEX sqlite_autoindex_submissions_completed_1 (id=?)
        14, 0, CORRELATED SCALAR SUBQUERY 1
        19, 14, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_submission_status_failed(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let query = r"
        SELECT
              id
            , prefix
            , chunks_total
            , chunks_done
            , chunk_size
            , metadata
            , ( SELECT json_group_object(metadata_key, metadata_value)
                FROM submissions_metadata
                WHERE submission_id = submissions_failed.id
              ) AS strategic_metadata
            , failed_at
            , failed_chunk_id
            , otel_trace_carrier
        FROM submissions_failed WHERE id = 1
        ";

        let explained = explain_query_plan(query, &mut conn).await;
        assert_non_regressing_query_plan(query, &explained);
        insta::assert_snapshot!(explained, @r"
        3, 0, SEARCH submissions_failed USING INDEX sqlite_autoindex_submissions_failed_1 (id=?)
        15, 0, CORRELATED SCALAR SUBQUERY 1
        20, 15, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_submission_status_cancelled(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let query = r"
        SELECT
              id
            , prefix
            , chunks_total
            , chunks_done
            , metadata
            , ( SELECT json_group_object(metadata_key, metadata_value)
                FROM submissions_metadata
                WHERE submission_id = submissions_cancelled.id
              ) AS strategic_metadata
            , cancelled_at
        FROM submissions_cancelled WHERE id = 1
        ";

        let explained = explain_query_plan(query, &mut conn).await;
        assert_non_regressing_query_plan(query, &explained);
        insta::assert_snapshot!(explained, @r"
        3, 0, SEARCH submissions_cancelled USING INDEX sqlite_autoindex_submissions_cancelled_1 (id=?)
        14, 0, CORRELATED SCALAR SUBQUERY 1
        19, 14, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_insert_submission(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();

        assert_eq!(count_submissions(&mut conn).await.unwrap(), u63::new(0));

        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
            ChunkSize::default(),
        )
        .unwrap();
        insert_submission(submission, chunks, &mut conn)
            .await
            .expect("insertion failed");

        assert_eq!(count_submissions(&mut conn).await.unwrap(), u63::new(1));
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_get_submission(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();
        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
            ChunkSize(1),
        )
        .unwrap();
        insert_submission(submission.clone(), chunks, &mut conn)
            .await
            .expect("insertion failed");

        let fetched_submission = get_submission(submission.id, &mut conn).await.unwrap();
        // When fetched from DB with no metadata rows, json_group_object returns '{}'.
        let submission = Submission {
            strategic_metadata: StrategicMetadataMap::default(),
            ..submission
        };
        assert_eq!(fetched_submission, submission);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
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
        assert_eq!(fetched_metadata, strategic_metadata);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_complete_submission_raw(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();
        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
            ChunkSize::default(),
        )
        .unwrap();
        insert_submission(submission.clone(), chunks, &mut conn)
            .await
            .expect("insertion failed");

        conn.transaction(move |mut tx| {
            Box::pin(async move { complete_submission_raw(submission.id, &mut tx).await })
        })
        .await
        .unwrap();

        assert_eq!(count_submissions(&mut conn).await.unwrap(), u63::new(0));
        assert_eq!(
            count_submissions_completed(&mut conn).await.unwrap(),
            u63::new(1)
        );
        assert_eq!(
            count_submissions_failed(&mut conn).await.unwrap(),
            u63::new(0)
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_fail_submission_raw(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();
        let (submission, chunks) = Submission::from_vec(
            vec![Some("foo".into()), Some("bar".into()), Some("baz".into())],
            None,
            ChunkSize::default(),
        )
        .unwrap();
        insert_submission(submission.clone(), chunks, &mut conn)
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
        assert_eq!(count_submissions(&mut conn).await.unwrap(), u63::new(0));
        assert_eq!(
            count_submissions_completed(&mut conn).await.unwrap(),
            u63::new(0)
        );
        assert_eq!(
            count_submissions_failed(&mut conn).await.unwrap(),
            u63::new(1)
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
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

        assert_eq!(
            count_submissions_failed(&mut conn).await.unwrap(),
            u63::new(5)
        );

        let mut conn2 = db.writer_conn().await.unwrap();
        cleanup_old(&mut conn2, cutoff_timestamp).await.unwrap();

        assert_eq!(
            count_submissions_failed(&mut conn).await.unwrap(),
            u63::new(2)
        );

        let _sub1 = submission_status(old_four_unfailed, &mut conn)
            .await
            .unwrap();
        let _sub2 = submission_status(old_four_unfailed, &mut conn)
            .await
            .unwrap();
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
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

        assert_eq!(count_submissions(&mut conn).await.unwrap(), u63::new(0));
        assert_eq!(
            count_submissions_completed(&mut conn).await.unwrap(),
            u63::new(1)
        );
        assert_eq!(
            count_submissions_failed(&mut conn).await.unwrap(),
            u63::new(0)
        );
    }

    /// Removes the given top-level key from a JSON object, panicking if it was not present.
    ///
    /// Used to simulate a JSON payload coming from an older peer that did not
    /// yet include the `strategic_metadata` field.
    fn without_key(value: serde_json::Value, key: &str) -> serde_json::Value {
        let mut value = value;
        let object = value.as_object_mut().expect("expected a JSON object");
        object
            .remove(key)
            .unwrap_or_else(|| panic!("expected key {key:?} to be present"));
        value
    }

    /// Ensures that a `Submission` serialized by an older peer (which did not
    /// include the `strategic_metadata` field) can still be deserialized,
    /// defaulting the field to an empty map rather than failing with a
    /// "missing field" error.
    #[test]
    fn strategic_metadata_is_optional_when_deserializing_submission() {
        let submission = Submission::new();
        let json = without_key(
            serde_json::to_value(&submission).unwrap(),
            "strategic_metadata",
        );
        let deserialized: Submission = serde_json::from_value(json).unwrap();
        assert_eq!(
            deserialized.strategic_metadata,
            StrategicMetadataMap::default()
        );
        assert_eq!(deserialized, submission);
    }

    /// Same as [`strategic_metadata_is_optional_when_deserializing_submission`],
    /// but for `SubmissionCompleted`.
    #[test]
    fn strategic_metadata_is_optional_when_deserializing_submission_completed() {
        let completed = SubmissionCompleted {
            id: SubmissionId(u63::new(1)),
            prefix: None,
            chunks_total: ChunkCount::zero(),
            chunk_size: ChunkSize::default(),
            metadata: None,
            strategic_metadata: StrategicMetadataMap::default(),
            completed_at: Utc::now(),
            otel_trace_carrier: String::new(),
        };
        let json = without_key(
            serde_json::to_value(&completed).unwrap(),
            "strategic_metadata",
        );
        let deserialized: SubmissionCompleted = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized, completed);
    }

    /// Same as [`strategic_metadata_is_optional_when_deserializing_submission`],
    /// but for `SubmissionFailed`.
    #[test]
    fn strategic_metadata_is_optional_when_deserializing_submission_failed() {
        let failed = SubmissionFailed {
            id: SubmissionId(u63::new(2)),
            prefix: None,
            chunks_total: ChunkCount::zero(),
            chunks_done: None,
            chunk_size: ChunkSize::default(),
            metadata: None,
            strategic_metadata: StrategicMetadataMap::default(),
            failed_at: Utc::now(),
            failed_chunk_id: ChunkIndex::zero(),
            otel_trace_carrier: String::new(),
        };
        let json = without_key(serde_json::to_value(&failed).unwrap(), "strategic_metadata");
        let deserialized: SubmissionFailed = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized, failed);
    }

    /// Same as [`strategic_metadata_is_optional_when_deserializing_submission`],
    /// but for `SubmissionCancelled`.
    #[test]
    fn strategic_metadata_is_optional_when_deserializing_submission_cancelled() {
        let cancelled = SubmissionCancelled {
            id: SubmissionId(u63::new(3)),
            prefix: None,
            chunks_total: ChunkCount::zero(),
            chunks_done: ChunkCount::zero(),
            metadata: None,
            strategic_metadata: StrategicMetadataMap::default(),
            cancelled_at: Utc::now(),
        };
        let json = without_key(
            serde_json::to_value(&cancelled).unwrap(),
            "strategic_metadata",
        );
        let deserialized: SubmissionCancelled = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized, cancelled);
    }
}

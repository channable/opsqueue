//! Dealing with `Chunk``s, the most fine-grained datatype the queue itself works with.
//!
//! While the smallest datatype is the 'Operation', the Opsqueue queue binary itself
//! only deals with _chunks_ of them. The operations inside of them are hidden and only read/written
//! from/to object store all the way in the client.
use chrono::{DateTime, Utc};

use serde::{Deserialize, Serialize};
use ux_serde::u63;

use super::errors::TryFromIntError;
use super::submission::SubmissionId;

/// Index of this particular chunk in a submission.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ChunkIndex(u63);

impl std::fmt::Debug for ChunkIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ChunkIndex").field(&self.0).finish()
    }
}

/// Another name for ChunkIndex, indicating that we're dealing with the _total count_ of chunks.
/// i.e. when you have a value of type ChunkCount, there is a high likelyhood
/// that there are [0..chunk_count) (note the half-open range) chunks to select from.
pub type ChunkCount = ChunkIndex;

/// The count of entries in each chunk.
///
/// Defaults to 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChunkSize(pub i64);

impl Default for ChunkSize {
    fn default() -> Self {
        ChunkSize(1)
    }
}

#[cfg(feature = "server-logic")]
impl sqlx::Decode<'_, sqlx::Sqlite> for ChunkSize {
    fn decode(
        value: <sqlx::Sqlite as sqlx::Database>::ValueRef<'_>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let inner = <Option<i64> as sqlx::Decode<'_, sqlx::Sqlite>>::decode(value)?;
        Ok(inner.map(ChunkSize).unwrap_or_default())
    }
}

impl From<Option<ChunkSize>> for ChunkSize {
    fn from(value: Option<ChunkSize>) -> Self {
        value.unwrap_or_default()
    }
}

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

impl PartialEq<u64> for ChunkIndex {
    fn eq(&self, other: &u64) -> bool {
        u64::from(*self) == *other
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

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "server-logic", derive(sqlx::FromRow))]
pub struct ChunkId {
    pub submission_id: SubmissionId,
    pub chunk_index: ChunkIndex,
}

impl From<(SubmissionId, ChunkIndex)> for ChunkId {
    fn from(value: (SubmissionId, ChunkIndex)) -> Self {
        Self {
            submission_id: value.0,
            chunk_index: value.1,
        }
    }
}

impl From<ChunkId> for (SubmissionId, ChunkIndex) {
    fn from(value: ChunkId) -> Self {
        (value.submission_id, value.chunk_index)
    }
}

impl std::fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkId")
            .field("submission_id", &u64::from(self.submission_id))
            .field("chunk_index", &u64::from(self.chunk_index.0))
            .finish()
    }
}

pub type Content = Option<Vec<u8>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "server-logic", derive(sqlx::FromRow))]
pub struct Chunk {
    pub submission_id: SubmissionId,
    pub chunk_index: ChunkIndex,
    pub input_content: Content,
    pub retries: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "server-logic", derive(sqlx::FromRow))]
pub struct ChunkCompleted {
    pub submission_id: SubmissionId,
    pub chunk_index: ChunkIndex,
    pub output_content: Content,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "server-logic", derive(sqlx::FromRow))]
pub struct ChunkFailed {
    pub submission_id: SubmissionId,
    pub chunk_index: ChunkIndex,
    pub input_content: Content,
    pub failure: String,
    pub failed_at: DateTime<Utc>,
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
    use crate::common::errors::{ChunkNotFound, DatabaseError, SubmissionNotFound, E};
    use crate::common::StrategicMetadataMap;
    use crate::db::{Connection, True, WriterConnection};
    use axum_prometheus::metrics::{counter, gauge};
    use sqlx::{query, query_as};
    use sqlx::{QueryBuilder, Sqlite};

    impl<'q> sqlx::Encode<'q, Sqlite> for super::ChunkIndex {
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
        fn decode(
            value: <Sqlite as sqlx::Database>::ValueRef<'q>,
        ) -> Result<Self, sqlx::error::BoxDynError> {
            let inner = <u64 as sqlx::Decode<'q, Sqlite>>::decode(value)?;
            let x = ChunkIndex::try_from(inner)?;
            Ok(x)
        }
    }

    #[tracing::instrument(skip(conn))]
    pub async fn insert_chunk(chunk: Chunk, mut conn: impl WriterConnection) -> sqlx::Result<()> {
        query!(
            "INSERT INTO chunks (submission_id, chunk_index, input_content) VALUES ($1, $2, $3)",
            chunk.submission_id,
            chunk.chunk_index,
            chunk.input_content
        )
        .execute(conn.get_inner())
        .await?;
        gauge!(crate::prometheus::CHUNKS_BACKLOG_GAUGE).increment(1);
        Ok(())
    }

    #[tracing::instrument(skip(conn))]
    pub async fn insert_chunk_metadata(
        chunk: Chunk,
        metadata_key: &[u8],
        metadata_value: &[u8],
        mut conn: impl WriterConnection,
    ) -> sqlx::Result<()> {
        query!(
            "
            INSERT INTO chunks_metadata
            ( submission_id
            , chunk_index
            , metadata_key
            , metadata_value
            )
            VALUES ($1, $2, $3, $4)
            ",
            chunk.submission_id,
            chunk.chunk_index,
            metadata_key,
            metadata_value,
        )
        .execute(conn.get_inner())
        .await?;
        Ok(())
    }

    #[tracing::instrument(skip(conn))]
    pub async fn complete_chunk(
        chunk_id: ChunkId,
        output_content: Option<Vec<u8>>,
        mut conn: impl WriterConnection,
    ) -> Result<(), E<DatabaseError, E<SubmissionNotFound, ChunkNotFound>>> {
        let _chunk_size: Result<ChunkSize, E<DatabaseError, E<SubmissionNotFound, ChunkNotFound>>> =
            conn.transaction(move |mut tx| {
                Box::pin(async move {
                    let completed_work =
                        complete_chunk_raw(chunk_id, output_content, &mut tx).await?;
                    crate::common::submission::db::maybe_complete_submission(
                        chunk_id.submission_id,
                        &mut tx,
                    )
                    .await
                    .map_err(|e| match e {
                        E::L(e) => E::L(e),
                        E::R(e) => E::R(E::L(e)),
                    })?;
                    Ok(completed_work.unwrap_or_default())
                })
            })
            .await;

        counter!(crate::prometheus::CHUNKS_COMPLETED_COUNTER).increment(1);
        Ok(())
    }

    /// This function MUST be called inside a transaction.
    #[tracing::instrument(skip(tx))]
    pub async fn complete_chunk_raw(
        chunk_id: ChunkId,
        output_content: Option<Vec<u8>>,
        mut tx: impl WriterConnection<Transaction = True>,
    ) -> sqlx::Result<Option<ChunkSize>> {
        let now = chrono::prelude::Utc::now();
        query!(
            "
        INSERT INTO chunks_completed
        (submission_id, chunk_index, output_content, completed_at)
        SELECT submission_id, chunk_index, $1, julianday($2) FROM chunks
        WHERE chunks.submission_id = $3 AND chunks.chunk_index = $4;

        DELETE FROM chunks WHERE chunks.submission_id = $5 AND chunks.chunk_index = $6
        RETURNING submission_id, chunk_index;
        ",
            output_content,
            now,
            chunk_id.submission_id,
            chunk_id.chunk_index,
            chunk_id.submission_id,
            chunk_id.chunk_index,
        )
        .fetch_one(tx.get_inner())
        .await?;
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
        sqlx::query_scalar!(
            "UPDATE submissions SET chunks_done = chunks_done + 1 WHERE submissions.id = $1 RETURNING submissions.chunk_size;",
            chunk_id.submission_id,
        )
        .fetch_one(tx.get_inner())
        .await
        .map(|opt| opt.map(ChunkSize))
    }

    #[tracing::instrument(skip(conn))]
    pub async fn retry_or_fail_chunk(
        chunk_id: ChunkId,
        failure: String,
        mut conn: impl WriterConnection,
    ) -> sqlx::Result<bool> {
        let failed_permanently = conn
            .transaction(move |mut tx| {
                Box::pin(async move {
                    const MAX_RETRIES: i64 = 10;
                    let ChunkId {
                        submission_id,
                        chunk_index,
                    } = chunk_id;
                    let fields = query!(
                        "
        UPDATE chunks SET retries = retries + 1
        WHERE submission_id = $1 AND chunk_index = $2
        RETURNING retries;
        ",
                        submission_id,
                        chunk_index
                    )
                    .fetch_one(tx.get_inner())
                    .await?;
                    tracing::trace!("Retries: {}", fields.retries);
                    if fields.retries >= MAX_RETRIES {
                        crate::common::submission::db::fail_submission_notx(
                            submission_id,
                            chunk_index,
                            failure,
                            &mut tx,
                        )
                        .await?;

                        Ok::<_, sqlx::Error>(true)
                    } else {
                        counter!(crate::prometheus::CHUNKS_RETRIED_COUNTER).increment(1);
                        // When retrying, the chunk re-enters ('stays') in the backlog,
                        // so we *don't* decrement the backlog gauge here.
                        Ok::<_, sqlx::Error>(false)
                    }
                })
            })
            .await?;
        Ok(failed_permanently)
    }

    #[tracing::instrument(skip(conn))]
    pub async fn move_chunk_to_failed_chunks(
        chunk_id: ChunkId,
        failure: String,
        mut conn: impl WriterConnection,
    ) -> sqlx::Result<()> {
        let now = chrono::prelude::Utc::now();
        let ChunkId {
            submission_id,
            chunk_index,
        } = chunk_id;
        query!("
    SAVEPOINT move_chunk_to_failed_chunks;

    INSERT INTO chunks_failed
    (submission_id, chunk_index, input_content, failure, failed_at)
    SELECT submission_id, chunk_index, input_content, $1, julianday($2) FROM chunks WHERE chunks.submission_id = $3 AND chunks.chunk_index = $4;

    DELETE FROM chunks WHERE chunks.submission_id = $5 AND chunks.chunk_index = $6 RETURNING *;

    RELEASE SAVEPOINT move_chunk_to_failed_chunks;
    ",
    failure,
    now,
    submission_id,
    chunk_index,
    submission_id,
    chunk_index,
    ).fetch_one(conn.get_inner()).await?;

        counter!(crate::prometheus::CHUNKS_FAILED_COUNTER).increment(1);
        gauge!(crate::prometheus::CHUNKS_BACKLOG_GAUGE).decrement(1);
        Ok(())
    }

    #[tracing::instrument(skip(conn))]
    pub async fn get_chunk(
        full_chunk_id: ChunkId,
        mut conn: impl Connection,
    ) -> sqlx::Result<Chunk> {
        query_as!(
            Chunk,
            r#"
        SELECT
            submission_id AS "submission_id: SubmissionId"
            , chunk_index AS "chunk_index: ChunkIndex"
            , input_content
            , retries
        FROM chunks WHERE submission_id = $1 AND chunk_index = $2
        "#,
            full_chunk_id.submission_id,
            full_chunk_id.chunk_index
        )
        .fetch_one(conn.get_inner())
        .await
    }

    #[tracing::instrument(skip(conn))]
    pub async fn get_chunk_completed(
        full_chunk_id: ChunkId,
        mut conn: impl Connection,
    ) -> sqlx::Result<ChunkCompleted> {
        query_as!(
            ChunkCompleted,
            r#"
        SELECT
            submission_id AS "submission_id: SubmissionId"
            , chunk_index AS "chunk_index: ChunkIndex"
            , output_content
            , completed_at AS "completed_at: DateTime<Utc>"
        FROM chunks_completed WHERE submission_id = $1 AND chunk_index = $2
        "#,
            full_chunk_id.submission_id,
            full_chunk_id.chunk_index
        )
        .fetch_one(conn.get_inner())
        .await
    }

    #[tracing::instrument(skip(conn))]
    pub async fn get_chunk_failed(
        full_chunk_id: ChunkId,
        mut conn: impl Connection,
    ) -> sqlx::Result<ChunkFailed> {
        query_as!(
            ChunkFailed,
            r#"
        SELECT
            submission_id AS "submission_id: SubmissionId"
            , chunk_index AS "chunk_index: ChunkIndex"
            , input_content
            , failure AS "failure: String"
            , failed_at AS "failed_at: DateTime<Utc>"
        FROM chunks_failed WHERE submission_id = $1 AND chunk_index = $2
        "#,
            full_chunk_id.submission_id,
            full_chunk_id.chunk_index
        )
        .fetch_one(conn.get_inner())
        .await
    }

    /// Retrieves the earlier stored strategic metadata.
    ///
    /// Primarily for testing and introspection.
    ///
    /// Be aware that the strategic metadata for individual chunks
    /// is cleaned up once the chunk is marked as completed or failed.
    /// (At that time it is still available on the submission level).
    pub async fn get_chunk_strategic_metadata(
        full_chunk_id: ChunkId,
        mut conn: impl Connection,
    ) -> Result<StrategicMetadataMap, DatabaseError> {
        use futures::{future, TryStreamExt};
        let metadata = query!(
            r#"
        SELECT metadata_key, metadata_value FROM chunks_metadata
        WHERE submission_id = $1 AND chunk_index = $2
        "#,
            full_chunk_id.submission_id,
            full_chunk_id.chunk_index,
        )
        .fetch(conn.get_inner())
        .and_then(|row| future::ok((row.metadata_key, row.metadata_value)))
        .try_collect()
        .await?;
        Ok(metadata)
    }

    #[tracing::instrument(skip(chunks, conn))]
    pub async fn insert_many_chunks(
        chunks: &[Chunk],
        mut conn: impl WriterConnection,
    ) -> sqlx::Result<()> {
        const ROWS_PER_QUERY: usize = 1000;

        // let start = std::time::Instant::now();
        let mut iter = chunks.iter().peekable();
        while iter.peek().is_some() {
            let query_chunks = iter.by_ref().take(ROWS_PER_QUERY);

            let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
                "INSERT INTO chunks (submission_id, chunk_index, input_content) ",
            );
            query_builder.push_values(query_chunks, |mut b, chunk| {
                b.push_bind(chunk.submission_id)
                    .push_bind(chunk.chunk_index)
                    .push_bind(chunk.input_content.clone());
            });
            let query = query_builder.build();

            query.execute(conn.get_inner()).await?;
        }

        Ok(())
    }

    pub async fn insert_many_chunks_metadata(
        chunks: &[Chunk],
        metadata: &StrategicMetadataMap,
        mut conn: impl WriterConnection,
    ) -> sqlx::Result<()> {
        use itertools::Itertools;
        const ROWS_PER_QUERY: usize = 1000;

        let mut iter = chunks.iter().cartesian_product(metadata).peekable();
        while iter.peek().is_some() {
            let query_rows = iter.by_ref().take(ROWS_PER_QUERY);

            let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
                "
            INSERT INTO chunks_metadata
            ( submission_id
            , chunk_index
            , metadata_key
            , metadata_value
            )
            ",
            );
            query_builder.push_values(query_rows, |mut b, (chunk, metadata)| {
                b.push_bind(chunk.submission_id)
                    .push_bind(chunk.chunk_index)
                    .push_bind(metadata.0)
                    .push_bind(metadata.1);
            });
            let query = query_builder.build();
            query.execute(conn.get_inner()).await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(conn))]
    pub async fn skip_remaining_chunks(
        submission_id: SubmissionId,
        mut conn: impl WriterConnection,
    ) -> sqlx::Result<()> {
        let now = chrono::prelude::Utc::now();

        let query_res = sqlx::query!("
    SAVEPOINT skip_remaining_chunks;

    INSERT INTO chunks_failed
    (submission_id, chunk_index, input_content, failure, skipped, failed_at)
    SELECT submission_id, chunk_index, input_content, '', 1, julianday($1) FROM chunks WHERE chunks.submission_id = $2;

    DELETE FROM chunks WHERE chunks.submission_id = $3;

    RELEASE SAVEPOINT skip_remaining_chunks;
    ",
    now,
    submission_id,
    submission_id,
    ).execute(conn.get_inner()).await?;

        counter!(crate::prometheus::CHUNKS_SKIPPED_COUNTER).increment(query_res.rows_affected());
        Ok(())
    }

    #[tracing::instrument(skip(db))]
    pub async fn count_chunks(mut db: impl Connection) -> sqlx::Result<u63> {
        let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks;")
            .fetch_one(db.get_inner())
            .await?;
        let count = u63::new(count.count as u64);
        Ok(count)
    }

    #[tracing::instrument(skip(db))]
    pub async fn count_chunks_completed(mut db: impl Connection) -> sqlx::Result<u63> {
        let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks_completed;")
            .fetch_one(db.get_inner())
            .await?;
        let count = u63::new(count.count as u64);
        Ok(count)
    }

    #[tracing::instrument(skip(db))]
    pub async fn count_chunks_failed(mut db: impl Connection) -> sqlx::Result<u63> {
        let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks_failed;")
            .fetch_one(db.get_inner())
            .await?;
        let count = u63::new(count.count as u64);
        Ok(count)
    }

    /// Looks up the number of operations in the backlog.
    ///
    /// An estimation that returns a slightly too high number,
    /// since we just count the number of chunks times their chunk size,
    /// meaning that we over-estimate the size of any 'final' chunk that is not full.
    pub async fn count_ops_in_backlog_estimate(mut db: impl Connection) -> sqlx::Result<f64> {
        let count = sqlx::query!("SELECT COALESCE(SUM(chunk_size * 1.0), 0.0) as count FROM chunks INNER JOIN submissions ON chunks.submission_id = submissions.id").fetch_one(db.get_inner()).await?.count;
        Ok(count)
    }
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {
    use crate::common::submission::db::insert_submission_raw;
    use crate::common::submission::{Submission, SubmissionStatus};
    use crate::common::StrategicMetadataMap;
    use crate::db::{Connection as _, WriterPool};

    use super::db::*;
    use super::*;

    #[sqlx::test]
    pub async fn test_insert_chunk(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();
        let chunk = Chunk::new(
            u63::new(1).into(),
            u63::new(0).into(),
            vec![1, 2, 3, 4, 5].into(),
        );

        assert!(count_chunks(&mut conn).await.unwrap() == u63::new(0));
        insert_chunk(chunk.clone(), &mut conn)
            .await
            .expect("Insert chunk failed");
        assert!(count_chunks(&mut conn).await.unwrap() == u63::new(1));
    }

    #[sqlx::test]
    pub async fn test_get_chunk(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();

        let chunk = Chunk::new(
            u63::new(1).into(),
            u63::new(0).into(),
            vec![1, 2, 3, 4, 5].into(),
        );
        insert_chunk(chunk.clone(), &mut conn)
            .await
            .expect("Insert chunk failed");

        let fetched_chunk = get_chunk((chunk.submission_id, chunk.chunk_index).into(), &mut conn)
            .await
            .unwrap();
        assert!(chunk == fetched_chunk);
    }

    #[sqlx::test]
    pub async fn test_complete_chunk_raw(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();

        let mut submission = Submission::new();
        submission.chunks_total = u63::new(1).into();
        submission.id = Submission::generate_id();
        let chunk = Chunk::new(
            submission.id,
            u63::new(0).into(),
            vec![1, 2, 3, 4, 5].into(),
        );

        insert_chunk(chunk.clone(), &mut conn)
            .await
            .expect("Insert chunk failed");

        insert_submission_raw(&submission, &mut conn).await.unwrap();

        conn.transaction(move |mut tx| {
            Box::pin(async move {
                complete_chunk_raw(
                    (chunk.submission_id, chunk.chunk_index).into(),
                    Some(vec![6, 7, 8, 9]),
                    &mut tx,
                )
                .await
            })
        })
        .await
        .expect("complete chunk failed");

        assert!(count_chunks(&mut conn).await.unwrap() == u63::new(0));
        assert!(count_chunks_completed(&mut conn).await.unwrap() == u63::new(1));
        assert!(count_chunks_failed(&mut conn).await.unwrap() == u63::new(0));
    }

    #[sqlx::test]
    pub async fn test_complete_chunk_raw_updates_submissions_chunk_total(db: sqlx::SqlitePool) {
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();
        let submission_id = crate::common::submission::db::insert_submission_from_chunks(
            None,
            vec![Some("first".into())],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            &mut conn,
        )
        .await
        .unwrap();

        assert!(count_chunks(&mut conn).await.unwrap() == u63::new(1));

        conn.transaction(move |mut tx| {
            Box::pin(async move {
                complete_chunk_raw(
                    (submission_id, u63::new(0).into()).into(),
                    Some(vec![6, 7, 8, 9]),
                    &mut tx,
                )
                .await
            })
        })
        .await
        .expect("complete chunk failed");

        let submission_status =
            crate::common::submission::db::submission_status(submission_id, &mut conn)
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
        let db = WriterPool::new(db);
        let mut conn = db.writer_conn().await.unwrap();
        let chunk = Chunk::new(
            u63::new(1).into(),
            u63::new(0).into(),
            vec![1, 2, 3, 4, 5].into(),
        );

        insert_chunk(chunk.clone(), &mut conn)
            .await
            .expect("Insert chunk failed");
        move_chunk_to_failed_chunks(
            (chunk.submission_id, chunk.chunk_index).into(),
            "Boom!".to_string(),
            &mut conn,
        )
        .await
        .expect("Succeed chunk failed");

        assert!(count_chunks(&mut conn).await.unwrap() == u63::new(0));
        assert!(count_chunks_completed(&mut conn).await.unwrap() == u63::new(0));
        assert!(count_chunks_failed(&mut conn).await.unwrap() == u63::new(1));
    }
}

use serde::{Deserialize, Serialize};

#[cfg(feature = "server-logic")]
use futures::stream::BoxStream;

use sqlx::Sqlite;
#[cfg(feature = "server-logic")]
use sqlx::{SqliteConnection, SqliteExecutor};

#[cfg(feature = "server-logic")]
use crate::common::chunk::Chunk;

use super::metastate::MetaState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    Oldest,
    Newest,
    Random,
    PreferDistinct {
        meta_key: String,
        underlying: Box<Strategy>,
    },
    // Custom(CustomStrategy), // TODO
}

#[cfg(feature = "server-logic")]
pub type ChunkStream<'a> = BoxStream<'a, Result<Chunk, sqlx::Error>>;

#[cfg(feature = "server-logic")]
impl Strategy {
    pub fn execute<'c>(&'c self, db_conn: &'c mut SqliteConnection, _metastate: &MetaState) -> ChunkStream<'c> {
        // use futures::{StreamExt, TryStreamExt};
        // use sqlx::FromRow;
        // self.query().build_query_as().fetch(db_conn)
        // self.query().build()
        // .fetch(db_conn)
        // .and_then(|row| async move { FromRow::from_row(&row) })
        // .boxed()
        match self {
            Strategy::Oldest => oldest_chunks_stream(db_conn),
            Strategy::Newest => newest_chunks_stream(db_conn),
            Strategy::Random => random_chunks_stream(db_conn),
            Strategy::PreferDistinct {
                meta_key,
                underlying,
                ..
            } => prefer_distinct_chunks_stream(db_conn, meta_key, underlying),
            // Strategy::Custom(CustomStrategy{implementation , ..}) => implementation(db_conn),
        }
        // use futures::{StreamExt, TryStreamExt};
        // futures::stream::once(async move { self.query().build_query_as() })
        // .then(|query| async move {
        //     query.fetch(&mut *db_conn)
        // })
        // .flatten()
        // .boxed()
    }

    pub fn query(&self) -> sqlx::QueryBuilder<'static, Sqlite> {
        match self {
            Strategy::Oldest => oldest_chunks_query(),
            _ => todo!(),
        }
    }
}

pub fn prefer_distinct_chunks_stream<'c>(
    db_conn: impl SqliteExecutor<'c> + 'c,
    meta_key: &'c str,
    underlying: &'c Strategy,
) -> ChunkStream<'c> {
    let meta_values = vec![];
    match underlying {
        Strategy::Oldest => sqlx::query_as(
            r#"
            SELECT
              chunks.submission_id AS "submission_id: _"
            , chunks.chunk_index AS "chunk_index: _"
            , chunks.input_content AS "input_content: _"
            , chunks.retries
            FROM chunks, chunks_metadata
            WHERE
            chunks.submission_id = chunks_metadata.submission_id
            AND
            chunks.chunk_index = chunks_metadata.chunk_index
            AND
            chunks_metadata.metadata_key = ?
            AND
            chunks_metadata.metadata_value NOT IN (?)

            ORDER BY submission_id ASC;
            "#,
        )
        .bind(meta_key)
        .bind(meta_values)
        .fetch(db_conn),

        Strategy::Newest => sqlx::query_as(
            r#"
            SELECT
              chunks.submission_id AS "submission_id: _"
            , chunks.chunk_index AS "chunk_index: _"
            , chunks.input_content AS "input_content: _"
            , chunks.retries
            FROM chunks, chunks_metadata
            WHERE
            chunks.submission_id = chunks_metadata.submission_id
            AND
            chunks.chunk_index = chunks_metadata.chunk_index
            AND
            chunks_metadata.metadata_key = ?
            AND
            chunks_metadata.metadata_value NOT IN (?)

            ORDER BY submission_id DESC;
            "#,
        )
        .bind(meta_key)
        .bind(meta_values)
        .fetch(db_conn),

        Strategy::Random => {
            let random_offset: u16 = rand::random();

            sqlx::query_as(
                r#"
            SELECT
              chunks.submission_id AS "submission_id: _"
            , chunks.chunk_index AS "chunk_index: _"
            , chunks.input_content AS "input_content: _"
            , chunks.retries
            FROM chunks, chunks_metadata
            WHERE
            chunks.submission_id = chunks_metadata.submission_id
            AND
            chunks.chunk_index = chunks_metadata.chunk_index
            AND
            chunks_metadata.metadata_key = ?
            AND
            chunks_metadata.metadata_value NOT IN (?)
            AND
            chunks_metadata.random_order >= ?

            UNION ALL

            SELECT
              chunks.submission_id AS "submission_id: _"
            , chunks.chunk_index AS "chunk_index: _"
            , chunks.input_content AS "input_content: _"
            , chunks.retries
            FROM chunks, chunks_metadata
            WHERE
            chunks.submission_id = chunks_metadata.submission_id
            AND
            chunks.chunk_index = chunks_metadata.chunk_index
            AND
            chunks_metadata.metadata_key = ?
            AND
            chunks_metadata.metadata_value NOT IN (?)
            AND
            chunks_metadata.random_order < ?;
            "#,
            )
            .bind(meta_key)
            .bind(meta_values.clone())
            .bind(random_offset)
            .bind(meta_key)
            .bind(meta_values)
            .bind(random_offset)
            .fetch(db_conn)
        }
        _ => {
            todo!()
        }
    }
}

// #[tracing::instrument]
#[cfg(feature = "server-logic")]
pub fn oldest_chunks_stream<'c>(db_conn: impl SqliteExecutor<'c> + 'c) -> ChunkStream<'c> {
    sqlx::query_as!(
        Chunk,
        r#"
        SELECT
            chunks.submission_id AS "submission_id: _"
            , chunks.chunk_index AS "chunk_index: _"
            , input_content AS "input_content: _"
            , retries
         FROM chunks
        ORDER BY submission_id ASC
    "#
    )
    .fetch(db_conn)
}

#[cfg(feature = "server-logic")]
pub fn oldest_chunks_query_as<'c>() -> sqlx::query::Map<
    'c,
    Sqlite,
    impl FnMut(sqlx::sqlite::SqliteRow) -> Result<Chunk, sqlx::Error>,
    sqlx::sqlite::SqliteArguments<'c>,
> {
    sqlx::query_as!(
        Chunk,
        r#"
        SELECT
            chunks.submission_id AS "submission_id: _"
            , chunks.chunk_index AS "chunk_index: _"
            , input_content AS "input_content: _"
            , retries
         FROM chunks
        ORDER BY submission_id ASC
    "#
    )
}

#[tracing::instrument]
#[cfg(feature = "server-logic")]
pub fn newest_chunks_stream<'c>(db_conn: impl SqliteExecutor<'c> + 'c) -> ChunkStream<'c> {
    sqlx::query_as!(
        Chunk,
        r#"
        SELECT
            chunks.submission_id AS "submission_id: _"
            , chunks.chunk_index AS "chunk_index: _"
            , input_content AS "input_content: _"
            , retries
        FROM chunks
        ORDER BY submission_id DESC
    "#
    )
    .fetch(db_conn)
}

/// Select one or more chunks in a random order.
///
/// We use two tricks to ensure this is both efficient and truly random:
/// 1. The random order is determined _on insert_, c.f. the VIRTUAL `random_order` column and associated index.
///    It ensures that chunks (especially consecutive chunks of the same submission)
///    are spread out evenly across the 16-bit range.
///    You can view this as 'shuffling the deck'.
/// 2. On _read_, pick a random permutation of this order.
///    This is implemented by picking a random offset,
///    reading everything after that, and then everything before that.
///    You can view this as 'cutting the deck'.
///
/// The goal of (1) is providing fairness with an efficient query.
/// The main goal of (2) is to reduce lock contention on the reserver, further improving performance.
///
/// A secondary goal of (2) is to make the random order non-deterministic,
/// which is mainly important when chunks need to be retried
/// (they should be retried 'whenever' rather than 'be placed back on the top of the deck')
///
/// Note that (2) without (1) would not be sufficiently random/fair.
/// (2) only works because data is spread out uniformly, which is not the case for the normal
/// `ChunkId`.
#[tracing::instrument]
#[cfg(feature = "server-logic")]
pub fn random_chunks_stream<'c>(db_conn: impl SqliteExecutor<'c> + 'c) -> ChunkStream<'c> {
    let random_offset: u16 = rand::random();
    // NOTE 1: Until https://github.com/launchbadge/sqlx/issues/1151
    // is fixed, we'll have to use the runtime `query_as`
    // rather than the compile-time checked one here.

    // NOTE 2: In theory the UNION ALL could be optimized and we lose the randomness.
    // The current version of SQLite doesn't do this, instead simply running the first and then the second.
    //
    // If it ever would become a problem, we can instead run two consecutive queries.
    let query = r#"
        SELECT
            submission_id
            , chunk_index
            , input_content
            , retries
        FROM chunks
        WHERE chunks.random_order >= ?
        UNION ALL
        SELECT
            submission_id
            , chunk_index
            , input_content
            , retries
        FROM chunks
        WHERE chunks.random_order < ?
    "#;
    sqlx::query_as(query)
        .bind(random_offset)
        .bind(random_offset)
        .fetch(db_conn)

    // Alternatively, we could do this 100% in SQL:
    /*

    sqlx::query_as!(ChunkId, r#"
    WITH const AS (select RANDOM() % 65536 AS random_offset)
    SELECT
          chunks.submission_id AS "submission_id: _"
        , chunks.chunk_index AS "chunk_index: _"
    FROM chunks, const
    WHERE chunks.random_order >= const.random_offset
    UNION ALL
    SELECT
          chunks.submission_id AS "submission_id: _"
        , chunks.chunk_index AS "chunk_index: _"
    FROM chunks, const
    WHERE chunks.random_order < const.random_offset;
    "#).fetch(db_conn)

    */
}

pub fn oldest_chunks_query() -> sqlx::QueryBuilder<'static, Sqlite> {
    sqlx::QueryBuilder::new(
        r#"
        SELECT
            chunks.submission_id AS "submission_id: _"
            , chunks.chunk_index AS "chunk_index: _"
            , input_content AS "input_content: _"
            , retries
         FROM chunks
        ORDER BY submission_id ASC
    "#,
    )
}

pub fn newest_chunks_query() -> sqlx::QueryBuilder<'static, Sqlite> {
    sqlx::QueryBuilder::new(
        r#"
        SELECT
            chunks.submission_id AS "submission_id: _"
            , chunks.chunk_index AS "chunk_index: _"
            , input_content AS "input_content: _"
            , retries
         FROM chunks
        ORDER BY submission_id DESC
    "#,
    )
}

pub fn random_chunks_query() -> sqlx::QueryBuilder<'static, Sqlite> {
    let random_offset: u16 = rand::random();
    let mut qb = sqlx::QueryBuilder::new("");

    qb.push(
        r#"
        SELECT
            submission_id
            , chunk_index
            , input_content
            , retries
        FROM chunks
        WHERE chunks.random_order >="#,
    );
    qb.push_bind(random_offset);
    qb.push(
        r#"
        UNION ALL
        SELECT
            submission_id
            , chunk_index
            , input_content
            , retries
        FROM chunks
        WHERE chunks.random_order < "#,
    );
    qb.push_bind(random_offset);

    qb
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {

    //     async fn explain(qb: sqlx::QueryBuilder<'_, Sqlite>, conn: &mut SqliteConnection) -> String {
    //         sqlx::raw_sql(&format!("EXPLAIN QUERY PLAN {}", qb.sql()))
    //             .fetch_all(&mut *conn)
    //             .await
    //             .unwrap()
    //             .into_iter()
    //             .map(|row| {
    //                 let id = row.get::<i64, &str>("id");
    //                 let parent = row.get::<i64, &str>("parent");
    //                 let detail = row.get::<String, &str>("detail");
    //                 format!("{}, {}, {}", id, parent, detail)
    //             })
    //             .join("\n")
    //     }

    //     async fn explain2(sql: &str, conn: &mut SqliteConnection) -> String {
    //         sqlx::raw_sql(&format!("EXPLAIN QUERY PLAN {}", sql))
    //             .fetch_all(&mut *conn)
    //             .await
    //             .unwrap()
    //             .into_iter()
    //             .map(|row| {
    //                 let id = row.get::<i64, &str>("id");
    //                 let parent = row.get::<i64, &str>("parent");
    //                 let detail = row.get::<String, &str>("detail");
    //                 format!("{}, {}, {}", id, parent, detail)
    //             })
    //             .join("\n")
    //     }

    //     #[sqlx::test]
    //     pub async fn test_query_plan_oldest(db: sqlx::SqlitePool) {
    //         let mut conn = db.acquire().await.unwrap();

    //         let explained = explain(oldest_chunks_query(), &mut *conn).await;
    //         assert_eq!(explained, "3, 0, SCAN chunks");
    //     }

    //     #[sqlx::test]
    //     pub async fn test_query_plan_oldest2(db: sqlx::SqlitePool) {
    //         let mut conn = db.acquire().await.unwrap();

    //         use sqlx::Execute;
    //         let sql = oldest_chunks_query_as().sql();
    //         let mut query = oldest_chunks_query_as();
    //         {
    //             use futures::StreamExt;
    //             let mut res = query.fetch(&mut *conn);
    //             while let Some(row) = res.next().await {
    //                 dbg!(row);
    //             }
    //         }
    //         assert!(false);

    //         let explained = explain2(oldest_chunks_query_as().sql(), &mut *conn).await;
    //         assert_eq!(explained, "3, 0, SCAN chunks");
    //     }

    //     #[sqlx::test]
    //     pub async fn test_query_plan_newest(db: sqlx::SqlitePool) {
    //         let mut conn = db.acquire().await.unwrap();

    //         let explained = explain(newest_chunks_query(), &mut *conn).await;
    //         assert_eq!(explained, "3, 0, SCAN chunks");
    //     }

    //     #[sqlx::test]
    //     pub async fn test_query_plan_random(db: sqlx::SqlitePool) {
    //         let mut conn = db.acquire().await.unwrap();

    //         let explained = explain(random_chunks_query(), &mut *conn).await;
    //         assert_eq!(
    //             explained,
    //             r#"1, 0, COMPOUND QUERY
    // 2, 1, LEFT-MOST SUBQUERY
    // 5, 2, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
    // 19, 1, UNION ALL
    // 22, 19, SEARCH chunks USING INDEX random_chunks_order (random_order<?)"#
    //         );
    //     }
}

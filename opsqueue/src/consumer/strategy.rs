#[cfg(feature = "server-logic")]
use std::string::ToString;

#[cfg(feature = "server-logic")]
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server-logic")]
use sqlx::{QueryBuilder, Sqlite};

#[cfg(feature = "server-logic")]
use crate::common::chunk::Chunk;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    Oldest,
    Newest,
    Random,
    PreferDistinct {
        meta_key: String,
        underlying: Box<Strategy>,
    },
}

pub struct MetaKeysIter<'a> {
    strategy: &'a Strategy,
}

impl<'a> MetaKeysIter<'a> {
    pub fn take(self) -> &'a Strategy {
        self.strategy
    }
}

impl<'a> Iterator for MetaKeysIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        match self.strategy {
            Strategy::Oldest | Strategy::Newest | Strategy::Random => None,
            Strategy::PreferDistinct {
                meta_key,
                underlying,
            } => {
                self.strategy = underlying.as_ref();
                Some(meta_key.as_str())
            }
        }
    }
}

impl Strategy {
    pub fn iter(&self) -> MetaKeysIter<'_> {
        MetaKeysIter { strategy: self }
    }
}

impl<'a> IntoIterator for &'a Strategy {
    type Item = &'a str;
    type IntoIter = MetaKeysIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(feature = "server-logic")]
impl Strategy {
    pub fn build_query<'a>(
        &self,
        qb: &'a mut QueryBuilder<'a, Sqlite>,
    ) -> &'a mut QueryBuilder<'a, Sqlite> {
        let qb = self.build_query_snippet(qb);
        tracing::trace!("sql: {:?}", qb.sql());
        qb
    }

    fn build_query_snippet_leaf<'a>(
        &self,
        qb: &'a mut QueryBuilder<'a, Sqlite>,
    ) -> &'a mut QueryBuilder<'a, Sqlite> {
        use Strategy::*;
        match self {
            Oldest => qb
                .push("SELECT * FROM chunks")
                .push(" WHERE opsqueue_is_reserved(submission_id, chunk_index) = 0")
                .push(" ORDER BY submission_id ASC"),
            Newest => qb
                .push("SELECT * FROM chunks")
                .push(" WHERE opsqueue_is_reserved(submission_id, chunk_index) = 0")
                .push(" ORDER BY submission_id DESC"),
            Random => {
                let random_offset: u16 = rand::random();
                qb.push("SELECT * FROM chunks WHERE random_order >= ")
                    .push_bind(random_offset)
                    .push(" AND opsqueue_is_reserved(submission_id, chunk_index) = 0")
                    .push(" UNION ALL SELECT * FROM chunks WHERE random_order < ")
                    .push_bind(random_offset)
                    .push(" AND opsqueue_is_reserved(submission_id, chunk_index) = 0")
            }
            PreferDistinct { .. } => {
                unreachable!("leaf builder should only be called on non-PreferDistinct")
            }
        }
    }

    fn build_query_snippet<'a>(
        &self,
        qb: &'a mut QueryBuilder<'a, Sqlite>,
    ) -> &'a mut QueryBuilder<'a, Sqlite> {
        use Strategy::*;
        match self {
            PreferDistinct { .. } => {
                let mut metaiter = self.iter();
                let prefer_distinct_metakeys = metaiter
                    .by_ref()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                let underlying = metaiter.take();

                let qb = qb.push("WITH underlying AS NOT MATERIALIZED (");
                let qb = underlying.build_query_snippet_leaf(qb);
                qb.push(")");

                for meta_key in &prefer_distinct_metakeys {
                  qb.push(", ");
                  qb.push(format_args!("{meta_key}_counts AS ("));
                  qb.push(" SELECT submission_id, opsqueue_metadata_count(");
                  qb.push_bind(meta_key.clone());
                  qb.push(", metadata_value) AS count");
                  qb.push(" FROM submissions_metadata WHERE metadata_key = ");
                  qb.push_bind(meta_key.clone());
                  qb.push(")");
                }

                qb.push(" SELECT underlying.* FROM underlying");
                for meta_key in &prefer_distinct_metakeys {
                  qb.push(format_args!(
                    " LEFT JOIN {meta_key}_counts ON underlying.submission_id = {meta_key}_counts.submission_id"
                  ));
                }
                qb.push(" ORDER BY ");
                for (i, meta_key) in prefer_distinct_metakeys.iter().enumerate() {
                    if i > 0 {
                        qb.push(", ");
                    }
                  qb.push(format_args!("{meta_key}_counts.count ASC NULLS FIRST"));
                }
                qb
            }
            Oldest | Newest | Random => self.build_query_snippet_leaf(qb),
        }
    }
}

#[cfg(feature = "server-logic")]
pub type ChunkStream<'a> = BoxStream<'a, Result<Chunk, sqlx::Error>>;

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {
    use itertools::Itertools;
    use libsqlite3_sys as ffi;
    use sqlformat::{format, FormatOptions, QueryParams};
    use sqlx::Row;
    use sqlx::{QueryBuilder, Sqlite, SqliteConnection};

    use super::*;
    use crate::common::chunk::ChunkSize;
    use crate::common::StrategicMetadataMap;

    unsafe extern "C" fn sqlite_reserved_chunk_lookup_noop(
        context: *mut ffi::sqlite3_context,
        _n_args: i32,
        _args: *mut *mut ffi::sqlite3_value,
    ) {
        ffi::sqlite3_result_int(context, 0);
    }

    unsafe extern "C" fn sqlite_metadata_count_lookup_noop(
        context: *mut ffi::sqlite3_context,
        _n_args: i32,
        _args: *mut *mut ffi::sqlite3_value,
    ) {
        ffi::sqlite3_result_null(context);
    }

    async fn register_lookup_noops(conn: &mut SqliteConnection) {
        let mut handle = conn.lock_handle().await.unwrap();
        let sqlite = handle.as_raw_handle().as_ptr();
        let function_name = b"opsqueue_is_reserved\0";
        let rc = unsafe {
            ffi::sqlite3_create_function_v2(
                sqlite,
                function_name.as_ptr().cast(),
                2,
                ffi::SQLITE_UTF8,
                std::ptr::null_mut(),
                Some(sqlite_reserved_chunk_lookup_noop),
                None,
                None,
                None,
            )
        };
        assert_eq!(rc, ffi::SQLITE_OK, "register opsqueue_is_reserved failed");

        let function_name = b"opsqueue_metadata_count\0";
        let rc = unsafe {
            ffi::sqlite3_create_function_v2(
                sqlite,
                function_name.as_ptr().cast(),
                2,
                ffi::SQLITE_UTF8,
                std::ptr::null_mut(),
                Some(sqlite_metadata_count_lookup_noop),
                None,
                None,
                None,
            )
        };
        assert_eq!(
            rc,
            ffi::SQLITE_OK,
            "register opsqueue_metadata_count failed"
        );
    }

    async fn explain(
        qb: &mut sqlx::QueryBuilder<'_, Sqlite>,
        conn: &mut SqliteConnection,
    ) -> String {
        let formatted_query = format(qb.sql(), &QueryParams::None, &FormatOptions::default());

        sqlx::raw_sql(&format!("EXPLAIN QUERY PLAN {}", formatted_query))
            .fetch_all(&mut *conn)
            .await
            .unwrap_or_else(|_| panic!("Invalid query: \n{}\n", formatted_query))
            .into_iter()
            .map(|row| {
                let id = row.get::<i64, &str>("id");
                let parent = row.get::<i64, &str>("parent");
                let detail = row.get::<String, &str>("detail");
                format!("{id}, {parent}, {detail}")
            })
            .join("\n")
    }

    fn assert_streaming_query(qb: &sqlx::QueryBuilder<'_, Sqlite>, explained: &str) {
        let query = qb.sql();
        assert!(!explained.contains("MATERIALIZED"), "Query should contain no materialization, but it did\n\nQuery: {query}\n\nPlan: \n\n {explained}");
        assert!(!explained.contains("B-TREE"), "Query should contain no temporary B-tree construction, but it did.\n\nQuery: {query}\n\nPlan: \n\n{explained}");
    }

    #[sqlx::test]
    pub async fn test_query_plan_oldest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        register_lookup_noops(&mut conn).await;
        let mut qb = QueryBuilder::new("");

        let qb = Strategy::Oldest.build_query(&mut qb);
        let options = FormatOptions::default();
        let formatted_query = format(qb.sql(), &QueryParams::None, &options);
        insta::assert_snapshot!(formatted_query, @"
        SELECT
          *
        FROM
          chunks
        WHERE
          opsqueue_is_reserved(submission_id, chunk_index) = 0
        ORDER BY
          submission_id ASC
        ");
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        assert_eq!(explained, "3, 0, SCAN chunks");
    }

    #[sqlx::test]
    pub async fn test_query_plan_newest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        register_lookup_noops(&mut conn).await;
        let mut qb = QueryBuilder::new("");

        let qb = Strategy::Newest.build_query(&mut qb);
        let options = FormatOptions::default();
        let formatted_query = format(qb.sql(), &QueryParams::None, &options);
        insta::assert_snapshot!(formatted_query, @"
        SELECT
          *
        FROM
          chunks
        WHERE
          opsqueue_is_reserved(submission_id, chunk_index) = 0
        ORDER BY
          submission_id DESC
        ");
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        assert_eq!(explained, "3, 0, SCAN chunks");
    }

    #[sqlx::test]
    pub async fn test_query_plan_random(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        register_lookup_noops(&mut conn).await;
        let mut qb = QueryBuilder::new("");

        let qb = Strategy::Random.build_query(&mut qb);

        let formatted_query = format(qb.sql(), &QueryParams::None, &FormatOptions::default());
        insta::assert_snapshot!(formatted_query, @"
        SELECT
          *
        FROM
          chunks
        WHERE
          random_order >= ?
          AND opsqueue_is_reserved(submission_id, chunk_index) = 0
        UNION ALL
        SELECT
          *
        FROM
          chunks
        WHERE
          random_order < ?
          AND opsqueue_is_reserved(submission_id, chunk_index) = 0
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_query(qb, &explained);
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        5, 2, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        26, 1, UNION ALL
        29, 26, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        ");
    }

    #[sqlx::test]
    pub async fn test_query_plan_prefer_distinct_oldest(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        register_lookup_noops(&mut conn).await;

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(Oldest),
        };
        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb);

        let formatted_query = format(qb.sql(), &QueryParams::None, &FormatOptions::default());
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying AS NOT MATERIALIZED (
          SELECT
            *
          FROM
            chunks
          WHERE
            opsqueue_is_reserved(submission_id, chunk_index) = 0
          ORDER BY
            submission_id ASC
        ),
        company_id_counts AS (
          SELECT
            submission_id,
            opsqueue_metadata_count(?, metadata_value) AS count
          FROM
            submissions_metadata
          WHERE
            metadata_key = ?
        )
        SELECT
          underlying.*
        FROM
          underlying
          LEFT JOIN company_id_counts ON underlying.submission_id = company_id_counts.submission_id
        ORDER BY
          company_id_counts.count ASC NULLS FIRST
        ");

        let explained = explain(qb, &mut conn).await;
        insta::assert_snapshot!(explained, @"
        5, 0, SCAN chunks USING INDEX random_chunks_order
        14, 0, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        41, 0, USE TEMP B-TREE FOR ORDER BY
        ");
    }

    #[sqlx::test]
    pub async fn test_query_plan_prefer_distinct_newest(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        register_lookup_noops(&mut conn).await;

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(Newest),
        };
        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb);

        let formatted_query = format(qb.sql(), &QueryParams::None, &FormatOptions::default());
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying AS NOT MATERIALIZED (
          SELECT
            *
          FROM
            chunks
          WHERE
            opsqueue_is_reserved(submission_id, chunk_index) = 0
          ORDER BY
            submission_id DESC
        ),
        company_id_counts AS (
          SELECT
            submission_id,
            opsqueue_metadata_count(?, metadata_value) AS count
          FROM
            submissions_metadata
          WHERE
            metadata_key = ?
        )
        SELECT
          underlying.*
        FROM
          underlying
          LEFT JOIN company_id_counts ON underlying.submission_id = company_id_counts.submission_id
        ORDER BY
          company_id_counts.count ASC NULLS FIRST
        ");

        let explained = explain(qb, &mut conn).await;
        insta::assert_snapshot!(explained, @"
        5, 0, SCAN chunks USING INDEX random_chunks_order
        14, 0, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        41, 0, USE TEMP B-TREE FOR ORDER BY
        ");
    }

    #[sqlx::test]
    pub async fn test_query_plan_prefer_distinct_random(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        register_lookup_noops(&mut conn).await;

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(Random),
        };
        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb);

        let formatted_query = format(qb.sql(), &QueryParams::None, &FormatOptions::default());
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying AS NOT MATERIALIZED (
          SELECT
            *
          FROM
            chunks
          WHERE
            random_order >= ?
            AND opsqueue_is_reserved(submission_id, chunk_index) = 0
          UNION ALL
          SELECT
            *
          FROM
            chunks
          WHERE
            random_order < ?
            AND opsqueue_is_reserved(submission_id, chunk_index) = 0
        ),
        company_id_counts AS (
          SELECT
            submission_id,
            opsqueue_metadata_count(?, metadata_value) AS count
          FROM
            submissions_metadata
          WHERE
            metadata_key = ?
        )
        SELECT
          underlying.*
        FROM
          underlying
          LEFT JOIN company_id_counts ON underlying.submission_id = company_id_counts.submission_id
        ORDER BY
          company_id_counts.count ASC NULLS FIRST
        ");

        let explained = explain(qb, &mut conn).await;
        insta::assert_snapshot!(explained, @"
        2, 0, CO-ROUTINE underlying
        3, 2, COMPOUND QUERY
        4, 3, LEFT-MOST SUBQUERY
        7, 4, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        28, 3, UNION ALL
        31, 28, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        56, 0, SCAN underlying
        59, 0, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        85, 0, USE TEMP B-TREE FOR ORDER BY
        ");
    }

    #[sqlx::test]
    pub async fn test_query_plan_prefer_distinct_nested(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        register_lookup_noops(&mut conn).await;

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(PreferDistinct {
                meta_key: "priority".to_string(),
                underlying: Box::new(Random),
            }),
        };

        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb);

        let formatted_query = format(qb.sql(), &QueryParams::None, &FormatOptions::default());
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying AS NOT MATERIALIZED (
          SELECT
            *
          FROM
            chunks
          WHERE
            random_order >= ?
            AND opsqueue_is_reserved(submission_id, chunk_index) = 0
          UNION ALL
          SELECT
            *
          FROM
            chunks
          WHERE
            random_order < ?
            AND opsqueue_is_reserved(submission_id, chunk_index) = 0
        ),
        company_id_counts AS (
          SELECT
            submission_id,
            opsqueue_metadata_count(?, metadata_value) AS count
          FROM
            submissions_metadata
          WHERE
            metadata_key = ?
        ),
        priority_counts AS (
          SELECT
            submission_id,
            opsqueue_metadata_count(?, metadata_value) AS count
          FROM
            submissions_metadata
          WHERE
            metadata_key = ?
        )
        SELECT
          underlying.*
        FROM
          underlying
          LEFT JOIN company_id_counts ON underlying.submission_id = company_id_counts.submission_id
          LEFT JOIN priority_counts ON underlying.submission_id = priority_counts.submission_id
        ORDER BY
          company_id_counts.count ASC NULLS FIRST,
          priority_counts.count ASC NULLS FIRST
        ");

        let explained = explain(qb, &mut conn).await;
        insta::assert_snapshot!(explained, @"
        2, 0, CO-ROUTINE underlying
        3, 2, COMPOUND QUERY
        4, 3, LEFT-MOST SUBQUERY
        7, 4, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        28, 3, UNION ALL
        31, 28, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        57, 0, SCAN underlying
        60, 0, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        70, 0, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        104, 0, USE TEMP B-TREE FOR ORDER BY
        ");
    }

    use crate::db::Connection;
    use futures::stream::TryStreamExt as _;

    #[sqlx::test]
    /// Tests whether the 'cutting the deck' technique is working
    ///
    /// We do this by checking whether two selects in a huge amount of available chunks
    /// give a different result.
    /// (There is a super tiny chance of this test flaking).
    pub async fn test_random_strategy_is_random(pool: sqlx::SqlitePool) {
        let db_pools = crate::db::DBPools::from_test_pool(&pool);

        let mut conn = db_pools.writer_conn().await.unwrap();
        let input_chunks: Vec<_> = (0..10_000).map(|x| Some(format!("{x}").into())).collect();
        crate::common::submission::db::insert_submission_from_chunks(
            None,
            input_chunks.clone(),
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            &mut conn,
        )
        .await
        .unwrap();

        let mut conn = db_pools.reader_conn().await.unwrap();
        register_lookup_noops(conn.get_inner()).await;
        let mut query_builder = QueryBuilder::default();
        let vals1: Vec<Chunk> = Strategy::Random
            .build_query(&mut query_builder)
            .build_query_as()
            .fetch(conn.get_inner())
            .try_collect()
            .await
            .unwrap();

        let mut query_builder = QueryBuilder::default();
        let vals2: Vec<Chunk> = Strategy::Random
            .build_query(&mut query_builder)
            .build_query_as()
            .fetch(conn.get_inner())
            .try_collect()
            .await
            .unwrap();

        assert!(vals1 != vals2)
    }
}

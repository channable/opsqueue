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

#[cfg(feature = "server-logic")]
impl Strategy {
    pub fn build_query<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<Sqlite>,
    ) -> &'a mut QueryBuilder<Sqlite> {
        let qb = self.build_query_snippet_returning_chunks(qb);
        tracing::trace!("sql: {:?}", qb.sql());
        qb
    }

    fn build_query_snippet_returning_chunks<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<Sqlite>,
    ) -> &'a mut QueryBuilder<Sqlite> {
        use Strategy::{Newest, Oldest, PreferDistinct, Random};
        let ffi_is_reserved = "opsqueue_is_reserved(chunks.submission_id, chunks.chunk_index) = 0";
        match self {
            Oldest => qb
                .push("SELECT * FROM chunks")
                .push(format!(" WHERE {ffi_is_reserved}"))
                .push(" ORDER BY submission_id ASC"),
            Newest => qb
                .push("SELECT * FROM chunks")
                .push(format!(" WHERE {ffi_is_reserved}"))
                .push(" ORDER BY submission_id DESC"),
            Random => Self::push_random_order_query(qb, "*", "chunks", Some(ffi_is_reserved)),
            PreferDistinct { .. } => {
                // Unique submission IDs from the underlying strategy.
                let qb = qb.push("WITH underlying_submission_ids AS MATERIALIZED (");
                let qb = self.build_query_snippet_returning_submission_ids(qb);
                qb.push(") ");
                // In SQLite, <foo> CROSS JOIN <bar> ON/WHERE does NOT produce N
                // x M rows, it acts as an INNER JOIN but forces the query
                // planner to use '<foo>' as the outer loop, preserving the
                // underlying sort order.
                // c.f. https://sqlite.org/optoverview.html#manual_control_of_query_plans_using_cross_join
                qb.push(format!(
                    " SELECT chunks.*
                        FROM underlying_submission_ids
                        CROSS JOIN chunks
                        ON chunks.submission_id = underlying_submission_ids.submission_id
                        AND {ffi_is_reserved}",
                ))
            }
        }
    }

    fn build_query_snippet_returning_submission_ids<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<Sqlite>,
    ) -> &'a mut QueryBuilder<Sqlite> {
        use Strategy::{Newest, Oldest, PreferDistinct, Random};
        match self {
            Oldest => qb.push("SELECT id as submission_id FROM submissions ORDER BY id ASC"),
            Newest => qb.push("SELECT id as submission_id FROM submissions ORDER BY id DESC"),
            Random => Self::push_random_order_query(qb, "id as submission_id", "submissions", None),
            PreferDistinct {
                meta_key,
                underlying,
            } => {
                // Unique submission IDs from the underlying strategy.
                let qb = qb.push("WITH inner AS NOT MATERIALIZED (");
                let qb = underlying.build_query_snippet_returning_submission_ids(qb);
                qb.push("),");
                // In-flight chunk count per submission, read via FFI.
                qb.push("counts AS (SELECT submission_id, opsqueue_metadata_count(");
                qb.push_bind(meta_key);
                qb.push(
                    ", metadata_value) AS count FROM submissions_metadata WHERE metadata_key = ",
                );
                qb.push_bind(meta_key);
                qb.push("),");
                // Submissions ranked by in-flight chunks. Submissions without a
                // value for this key get a NULL count and so are ranked first.
                qb.push(
                    // MATERIALIZED is necessary to preserve the order.
                    "ranked_submissions AS MATERIALIZED (
                        SELECT inner.submission_id
                        FROM inner
                        LEFT JOIN counts c ON inner.submission_id = c.submission_id
                        ORDER BY c.count ASC NULLS FIRST
                    )",
                );
                qb.push(" SELECT submission_id FROM ranked_submissions")
            }
        }
    }

    /// Append a query snippet to select from the `random_order` column on the
    /// given table using the "cutting the deck" technique.
    fn push_random_order_query<'a>(
        qb: &'a mut QueryBuilder<Sqlite>,
        columns: &str,
        table_name: &str,
        condition: Option<&str>,
    ) -> &'a mut QueryBuilder<Sqlite> {
        let random_offset: u16 = rand::random();
        let push_select = |qb: &mut QueryBuilder<Sqlite>, operator: &str| {
            qb.push(format!(
                "SELECT {columns} FROM {table_name} WHERE random_order {operator} "
            ))
            .push_bind(random_offset);
            if let Some(condition_) = condition {
                qb.push(format!(" AND {condition_}"));
            }
        };
        push_select(qb, ">=");
        qb.push(" UNION ALL ");
        push_select(qb, "<");
        qb
    }
}

#[cfg(feature = "server-logic")]
pub type ChunkStream<'a> = BoxStream<'a, Result<Chunk, sqlx::Error>>;

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {
    use super::*;
    use crate::common::StrategicMetadataMap;
    use crate::common::chunk::ChunkSize;
    use itertools::Itertools;
    use libsqlite3_sys as ffi;
    use sqlformat::{FormatOptions, QueryParams, format};
    use sqlx::Row;
    use sqlx::{QueryBuilder, Sqlite, SqliteConnection};

    unsafe extern "C" fn sqlite_reserved_chunk_lookup_noop(
        context: *mut ffi::sqlite3_context,
        _n_args: i32,
        _args: *mut *mut ffi::sqlite3_value,
    ) {
        unsafe { ffi::sqlite3_result_int(context, 0) };
    }

    unsafe extern "C" fn sqlite_metadata_count_lookup_noop(
        context: *mut ffi::sqlite3_context,
        _n_args: i32,
        _args: *mut *mut ffi::sqlite3_value,
    ) {
        unsafe { ffi::sqlite3_result_null(context) };
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

    async fn explain(qb: &mut sqlx::QueryBuilder<Sqlite>, conn: &mut SqliteConnection) -> String {
        let formatted_query = format(
            qb.sql().as_str(),
            &QueryParams::None,
            &FormatOptions::default(),
        );
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "EXPLAIN QUERY PLAN {formatted_query}"
        )))
        .fetch_all(&mut *conn)
        .await
        .unwrap_or_else(|_| panic!("Invalid query: \n{formatted_query}\n"))
        .into_iter()
        .map(|row| {
            let id = row.get::<i64, &str>("id");
            let parent = row.get::<i64, &str>("parent");
            let detail = row.get::<String, &str>("detail");
            format!("{id}, {parent}, {detail}")
        })
        .join("\n")
    }

    fn assert_streaming_query(qb: &sqlx::QueryBuilder<Sqlite>, explained: &str) {
        let query_binding = qb.sql();
        let query = query_binding.as_str();
        assert!(
            !explained.contains("MATERIALIZED"),
            "Query should contain no materialization, but it did\n\nQuery: {query}\n\nPlan: \n\n {explained}"
        );
        assert!(
            !explained.contains("B-TREE"),
            "Query should contain no temporary B-tree construction, but it did.\n\nQuery: {query}\n\nPlan: \n\n{explained}"
        );
    }

    /// A weaker version of `assert_streaming_query`, for `PreferDistinct`.
    ///
    /// `PreferDistinct` cannot stream: to rank submissions by how many of their
    /// chunks are already in flight, it has to sort the `submissions` table. We
    /// accept that cost, because there are fewer submissions than chunks.
    ///
    /// What we do not accept is doing the same to `chunks`, so we only require
    /// that `chunks` is reached by an index seek.
    fn assert_streaming_chunks(qb: &sqlx::QueryBuilder<Sqlite>, explained: &str) {
        let query_binding = qb.sql();
        let query = query_binding.as_str();
        assert!(
            !explained.contains("SCAN chunks"),
            "Query should never scan the whole `chunks` backlog, but it did.\n\nQuery: {query}\n\nPlan: \n\n{explained}"
        );
        assert!(
            explained.contains("SEARCH chunks"),
            "Query should reach `chunks` via an index seek, but it did not.\n\nQuery: {query}\n\nPlan: \n\n{explained}"
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_oldest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        register_lookup_noops(&mut conn).await;
        let mut qb = QueryBuilder::new("");

        let qb = Strategy::Oldest.build_query(&mut qb);
        let options = FormatOptions::default();
        let formatted_query = format(qb.sql().as_str(), &QueryParams::None, &options);
        insta::assert_snapshot!(formatted_query, @"
        SELECT
          *
        FROM
          chunks
        WHERE
          opsqueue_is_reserved(chunks.submission_id, chunks.chunk_index) = 0
        ORDER BY
          submission_id ASC
        ");
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        assert_eq!(explained, "3, 0, SCAN chunks");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_newest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        register_lookup_noops(&mut conn).await;
        let mut qb = QueryBuilder::new("");

        let qb = Strategy::Newest.build_query(&mut qb);
        let options = FormatOptions::default();
        let formatted_query = format(qb.sql().as_str(), &QueryParams::None, &options);
        insta::assert_snapshot!(formatted_query, @"
        SELECT
          *
        FROM
          chunks
        WHERE
          opsqueue_is_reserved(chunks.submission_id, chunks.chunk_index) = 0
        ORDER BY
          submission_id DESC
        ");
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        assert_eq!(explained, "3, 0, SCAN chunks");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_random(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        register_lookup_noops(&mut conn).await;
        let mut qb = QueryBuilder::new("");

        let qb = Strategy::Random.build_query(&mut qb);

        let formatted_query = format(
            qb.sql().as_str(),
            &QueryParams::None,
            &FormatOptions::default(),
        );
        insta::assert_snapshot!(formatted_query, @"
        SELECT
          *
        FROM
          chunks
        WHERE
          random_order >= ?
          AND opsqueue_is_reserved(chunks.submission_id, chunks.chunk_index) = 0
        UNION ALL
        SELECT
          *
        FROM
          chunks
        WHERE
          random_order < ?
          AND opsqueue_is_reserved(chunks.submission_id, chunks.chunk_index) = 0
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

    #[sqlx::test(migrator = "crate::MIGRATOR")]
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

        let formatted_query = format(
            qb.sql().as_str(),
            &QueryParams::None,
            &FormatOptions::default(),
        );
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying_submission_ids AS MATERIALIZED (
          WITH
          inner AS NOT MATERIALIZED (
            SELECT
              id as submission_id
            FROM
              submissions
            ORDER BY
              id ASC
          ),
          counts AS (
            SELECT
              submission_id,
              opsqueue_metadata_count(?, metadata_value) AS count
            FROM
              submissions_metadata
            WHERE
              metadata_key = ?
          ),
          ranked_submissions AS MATERIALIZED (
            SELECT
              inner.submission_id
            FROM
              inner
              LEFT JOIN counts c ON inner.submission_id = c.submission_id
            ORDER BY
              c.count ASC NULLS FIRST
          )
          SELECT
            submission_id
          FROM
            ranked_submissions
        )
        SELECT
          chunks.*
        FROM
          underlying_submission_ids
          CROSS JOIN chunks ON chunks.submission_id = underlying_submission_ids.submission_id
          AND opsqueue_is_reserved(chunks.submission_id, chunks.chunk_index) = 0
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE underlying_submission_ids
        6, 3, MATERIALIZE ranked_submissions
        11, 6, SCAN submissions USING COVERING INDEX sqlite_autoindex_submissions_1
        13, 6, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        35, 6, USE TEMP B-TREE FOR ORDER BY
        47, 3, SCAN ranked_submissions
        58, 0, SCAN underlying_submission_ids
        60, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
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

        let formatted_query = format(
            qb.sql().as_str(),
            &QueryParams::None,
            &FormatOptions::default(),
        );
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying_submission_ids AS MATERIALIZED (
          WITH
          inner AS NOT MATERIALIZED (
            SELECT
              id as submission_id
            FROM
              submissions
            ORDER BY
              id DESC
          ),
          counts AS (
            SELECT
              submission_id,
              opsqueue_metadata_count(?, metadata_value) AS count
            FROM
              submissions_metadata
            WHERE
              metadata_key = ?
          ),
          ranked_submissions AS MATERIALIZED (
            SELECT
              inner.submission_id
            FROM
              inner
              LEFT JOIN counts c ON inner.submission_id = c.submission_id
            ORDER BY
              c.count ASC NULLS FIRST
          )
          SELECT
            submission_id
          FROM
            ranked_submissions
        )
        SELECT
          chunks.*
        FROM
          underlying_submission_ids
          CROSS JOIN chunks ON chunks.submission_id = underlying_submission_ids.submission_id
          AND opsqueue_is_reserved(chunks.submission_id, chunks.chunk_index) = 0
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE underlying_submission_ids
        6, 3, MATERIALIZE ranked_submissions
        11, 6, SCAN submissions USING COVERING INDEX sqlite_autoindex_submissions_1
        13, 6, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        35, 6, USE TEMP B-TREE FOR ORDER BY
        47, 3, SCAN ranked_submissions
        58, 0, SCAN underlying_submission_ids
        60, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
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

        let formatted_query = format(
            qb.sql().as_str(),
            &QueryParams::None,
            &FormatOptions::default(),
        );
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying_submission_ids AS MATERIALIZED (
          WITH
          inner AS NOT MATERIALIZED (
            SELECT
              id as submission_id
            FROM
              submissions
            WHERE
              random_order >= ?
            UNION ALL
            SELECT
              id as submission_id
            FROM
              submissions
            WHERE
              random_order < ?
          ),
          counts AS (
            SELECT
              submission_id,
              opsqueue_metadata_count(?, metadata_value) AS count
            FROM
              submissions_metadata
            WHERE
              metadata_key = ?
          ),
          ranked_submissions AS MATERIALIZED (
            SELECT
              inner.submission_id
            FROM
              inner
              LEFT JOIN counts c ON inner.submission_id = c.submission_id
            ORDER BY
              c.count ASC NULLS FIRST
          )
          SELECT
            submission_id
          FROM
            ranked_submissions
        )
        SELECT
          chunks.*
        FROM
          underlying_submission_ids
          CROSS JOIN chunks ON chunks.submission_id = underlying_submission_ids.submission_id
          AND opsqueue_is_reserved(chunks.submission_id, chunks.chunk_index) = 0
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE underlying_submission_ids
        6, 3, MATERIALIZE ranked_submissions
        8, 6, CO-ROUTINE inner
        9, 8, COMPOUND QUERY
        10, 9, LEFT-MOST SUBQUERY
        13, 10, SEARCH submissions USING INDEX random_submissions_order (random_order>?)
        22, 9, UNION ALL
        25, 22, SEARCH submissions USING INDEX random_submissions_order (random_order<?)
        39, 6, SCAN inner
        42, 6, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        65, 6, USE TEMP B-TREE FOR ORDER BY
        77, 3, SCAN ranked_submissions
        88, 0, SCAN underlying_submission_ids
        90, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
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

        let formatted_query = format(
            qb.sql().as_str(),
            &QueryParams::None,
            &FormatOptions::default(),
        );
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying_submission_ids AS MATERIALIZED (
          WITH
          inner AS NOT MATERIALIZED (
            WITH
            inner AS NOT MATERIALIZED (
              SELECT
                id as submission_id
              FROM
                submissions
              WHERE
                random_order >= ?
              UNION ALL
              SELECT
                id as submission_id
              FROM
                submissions
              WHERE
                random_order < ?
            ),
            counts AS (
              SELECT
                submission_id,
                opsqueue_metadata_count(?, metadata_value) AS count
              FROM
                submissions_metadata
              WHERE
                metadata_key = ?
            ),
            ranked_submissions AS MATERIALIZED (
              SELECT
                inner.submission_id
              FROM
                inner
                LEFT JOIN counts c ON inner.submission_id = c.submission_id
              ORDER BY
                c.count ASC NULLS FIRST
            )
            SELECT
              submission_id
            FROM
              ranked_submissions
          ),
          counts AS (
            SELECT
              submission_id,
              opsqueue_metadata_count(?, metadata_value) AS count
            FROM
              submissions_metadata
            WHERE
              metadata_key = ?
          ),
          ranked_submissions AS MATERIALIZED (
            SELECT
              inner.submission_id
            FROM
              inner
              LEFT JOIN counts c ON inner.submission_id = c.submission_id
            ORDER BY
              c.count ASC NULLS FIRST
          )
          SELECT
            submission_id
          FROM
            ranked_submissions
        )
        SELECT
          chunks.*
        FROM
          underlying_submission_ids
          CROSS JOIN chunks ON chunks.submission_id = underlying_submission_ids.submission_id
          AND opsqueue_is_reserved(chunks.submission_id, chunks.chunk_index) = 0
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE underlying_submission_ids
        6, 3, MATERIALIZE ranked_submissions
        9, 6, MATERIALIZE ranked_submissions
        11, 9, CO-ROUTINE inner
        12, 11, COMPOUND QUERY
        13, 12, LEFT-MOST SUBQUERY
        16, 13, SEARCH submissions USING INDEX random_submissions_order (random_order>?)
        25, 12, UNION ALL
        28, 25, SEARCH submissions USING INDEX random_submissions_order (random_order<?)
        42, 9, SCAN inner
        45, 9, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        68, 9, USE TEMP B-TREE FOR ORDER BY
        82, 6, SCAN ranked_submissions
        84, 6, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        107, 6, USE TEMP B-TREE FOR ORDER BY
        119, 3, SCAN ranked_submissions
        130, 0, SCAN underlying_submission_ids
        132, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
        ");
    }

    use crate::db::Connection;
    use futures::stream::TryStreamExt as _;

    #[sqlx::test(migrator = "crate::MIGRATOR")]
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

        assert_ne!(vals1, vals2);
    }
}

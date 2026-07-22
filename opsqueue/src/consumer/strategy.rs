#[cfg(feature = "server-logic")]
use futures::stream::BoxStream;

use serde::{Deserialize, Serialize};

#[cfg(feature = "server-logic")]
use sqlx::{QueryBuilder, Sqlite};

#[cfg(feature = "server-logic")]
use crate::common::chunk::Chunk;

#[cfg(feature = "server-logic")]
use super::dispatcher::metastate::MetaState;

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
        metastate: &MetaState,
    ) -> &'a mut QueryBuilder<Sqlite> {
        let qb = self.build_query_snippet_returning_chunks(qb, metastate);
        tracing::trace!("sql: {:?}", qb.sql());
        qb
    }

    fn build_query_snippet_returning_chunks<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<Sqlite>,
        metastate: &MetaState,
    ) -> &'a mut QueryBuilder<Sqlite> {
        use Strategy::{Newest, Oldest, PreferDistinct, Random};
        match self {
            Oldest => qb
                .push("SELECT * FROM chunks")
                .push(" WHERE opsqueue_is_reserved(submission_id, chunk_index) = 0")
                .push(" ORDER BY submission_id ASC"),
            Newest => qb
                .push("SELECT * FROM chunks")
                .push(" WHERE opsqueue_is_reserved(submission_id, chunk_index) = 0")
                .push(" ORDER BY submission_id DESC"),
            Random => Self::push_random_order_query(
                qb,
                "*",
                "chunks",
                Some("opsqueue_is_reserved(submission_id, chunk_index) = 0"),
            ),
            PreferDistinct { .. } => {
                qb.push("WITH underlying_submissions AS MATERIALIZED (");
                let qb = self.build_query_snippet_returning_submission_ids(qb, metastate);
                qb.push(") ");
                // In SQLite, CROSS JOIN <table> ON/WHERE does NOT produce N
                // x M rows, it acts as an INNER JOIN forcing the query
                // planner to use '<table> as the outer loop, preserving its
                // sort order.
                // c.f. https://sqlite.org/optoverview.html#manual_control_of_query_plans_using_cross_join
                qb.push(
                    " SELECT chunks.*
                        FROM underlying_submissions
                        CROSS JOIN chunks
                        WHERE opsqueue_is_reserved(chunks.submission_id, chunk_index) = 0
                        AND chunks.submission_id = underlying_submissions.submission_id"
                )
            }
        }
    }

    /// Append a query snippet resulting in an ordered "`submission_id`" column.
    fn build_query_snippet_returning_submission_ids<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<Sqlite>,
        metastate: &MetaState,
    ) -> &'a mut QueryBuilder<Sqlite> {
        use Strategy::{Newest, Oldest, PreferDistinct, Random};
        match self {
            Oldest => qb.push("SELECT id AS submission_id FROM submissions ORDER BY id ASC"),
            Newest => qb.push("SELECT id AS submission_id FROM submissions ORDER BY id DESC"),
            Random => Self::push_random_order_query(qb, "id as submission_id", "submissions", None),
            PreferDistinct {
                meta_key,
                underlying,
            } => {
                // Unique submission IDs from the underlying strategy.
                let qb = qb.push("WITH inner AS NOT MATERIALIZED (");
                let qb = underlying.build_query_snippet_returning_submission_ids(qb, metastate);
                qb.push("),");
                // Count of in-flight chunks per submission.
                qb.push("counts AS (SELECT key, value as count FROM json_each(");
                match metastate.get(meta_key) {
                    None => {
                        tracing::trace!("No metastate field for key: {meta_key}");
                        qb.push_bind("{}");
                    }
                    Some(field) => {
                        let counts_map: std::collections::HashMap<_, _> = field
                            .vals_to_counts
                            .iter()
                            .map(|kv| (*kv.key(), *kv.value()))
                            .collect();
                        let counts_json =
                            serde_json::to_string(&counts_map).expect("Always valid JSON");
                        tracing::trace!(
                            "Granular active counts for PreferDistinct: {counts_json:?}"
                        );
                        qb.push_bind(counts_json);
                    }
                }
                qb.push(")),");
                // Submissions ranked by in-flight chunks.
                qb.push(
                    // MATERIALIZED is necessary to preserve the order.
                    "ranked_submissions AS MATERIALIZED (
                        SELECT inner.submission_id
                        FROM inner
                        LEFT JOIN submissions_metadata sm
                            ON inner.submission_id = sm.submission_id
                            AND sm.metadata_key = ",
                );
                qb.push_bind(meta_key);
                qb.push(
                    " LEFT JOIN counts ON sm.metadata_value = counts.key
                        ORDER BY counts.count ASC NULLS FIRST
                    )",
                );
                qb.push(" SELECT submission_id FROM ranked_submissions")
            }
        }
    }

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
    use itertools::Itertools;
    use libsqlite3_sys as ffi;
    use sqlformat::{FormatOptions, QueryParams, format};
    use sqlx::Row;
    use sqlx::{QueryBuilder, Sqlite, SqliteConnection};

    use super::*;
    use crate::common::StrategicMetadataMap;
    use crate::common::chunk::ChunkSize;

    unsafe extern "C" fn sqlite_reserved_chunk_lookup_noop(
        context: *mut ffi::sqlite3_context,
        _n_args: i32,
        _args: *mut *mut ffi::sqlite3_value,
    ) {
        unsafe {
            ffi::sqlite3_result_int(context, 0);
        }
    }

    pub async fn register_reserved_lookup_noop(conn: &mut SqliteConnection) {
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

    /// Weaker invariant than `assert_streaming_query`, used by `PreferDistinct`
    /// which deliberately materializes and sorts the set of in-flight
    /// submissions to rank them by metadata value. That bounded sort is
    /// considered acceptable; what must not happen is a full scan or sort of
    /// the potentially very large `chunks`.
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
        register_reserved_lookup_noop(&mut conn).await;
        let mut qb = QueryBuilder::new("");
        let metastate = MetaState::default();

        let qb = Strategy::Oldest.build_query(&mut qb, &metastate);
        let options = FormatOptions::default();
        let formatted_query = format(qb.sql().as_str(), &QueryParams::None, &options);
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

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_newest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
        let mut qb = QueryBuilder::new("");
        let metastate = MetaState::default();

        let qb = Strategy::Newest.build_query(&mut qb, &metastate);
        let options = FormatOptions::default();
        let formatted_query = format(qb.sql().as_str(), &QueryParams::None, &options);
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

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_random(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
        let metastate = MetaState::default();
        let mut qb = QueryBuilder::new("");

        let qb = Strategy::Random.build_query(&mut qb, &metastate);

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
        insta::assert_snapshot!(explained, @"
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
        register_reserved_lookup_noop(&mut conn).await;
        let metastate = MetaState::default();

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(Oldest),
        };
        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb, &metastate);

        let formatted_query = format(
            qb.sql().as_str(),
            &QueryParams::None,
            &FormatOptions::default(),
        );
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying_submissions AS MATERIALIZED (
          WITH
          inner AS NOT MATERIALIZED (
            SELECT
              id AS submission_id
            FROM
              submissions
            ORDER BY
              id ASC
          ),
          counts AS (
            SELECT
              key,
              value as count
            FROM
              json_each(?)
          ),
          ranked_submissions AS MATERIALIZED (
            SELECT
              inner.submission_id
            FROM
              inner
              LEFT JOIN submissions_metadata sm ON inner.submission_id = sm.submission_id
              AND sm.metadata_key = ?
              LEFT JOIN counts ON sm.metadata_value = counts.key
            ORDER BY
              counts.count ASC NULLS FIRST
          )
          SELECT
            submission_id
          FROM
            ranked_submissions
        )
        SELECT
          chunks.*
        FROM
          underlying_submissions
          CROSS JOIN chunks
        WHERE
          chunks.submission_id = underlying_submissions.submission_id
          AND opsqueue_is_reserved(chunks.submission_id, chunk_index) = 0
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE underlying_submissions
        6, 3, MATERIALIZE ranked_submissions
        9, 6, MATERIALIZE counts
        12, 9, SCAN json_each VIRTUAL TABLE INDEX 1:
        28, 6, SCAN submissions USING COVERING INDEX sqlite_autoindex_submissions_1
        30, 6, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        41, 6, SCAN counts LEFT-JOIN
        61, 6, USE TEMP B-TREE FOR ORDER BY
        73, 3, SCAN ranked_submissions
        84, 0, SCAN underlying_submissions
        86, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_prefer_distinct_newest(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
        let metastate = MetaState::default();

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(Newest),
        };
        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb, &metastate);

        let formatted_query = format(
            qb.sql().as_str(),
            &QueryParams::None,
            &FormatOptions::default(),
        );
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying_submissions AS MATERIALIZED (
          WITH
          inner AS NOT MATERIALIZED (
            SELECT
              id AS submission_id
            FROM
              submissions
            ORDER BY
              id DESC
          ),
          counts AS (
            SELECT
              key,
              value as count
            FROM
              json_each(?)
          ),
          ranked_submissions AS MATERIALIZED (
            SELECT
              inner.submission_id
            FROM
              inner
              LEFT JOIN submissions_metadata sm ON inner.submission_id = sm.submission_id
              AND sm.metadata_key = ?
              LEFT JOIN counts ON sm.metadata_value = counts.key
            ORDER BY
              counts.count ASC NULLS FIRST
          )
          SELECT
            submission_id
          FROM
            ranked_submissions
        )
        SELECT
          chunks.*
        FROM
          underlying_submissions
          CROSS JOIN chunks
        WHERE
          chunks.submission_id = underlying_submissions.submission_id
          AND opsqueue_is_reserved(chunks.submission_id, chunk_index) = 0
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE underlying_submissions
        6, 3, MATERIALIZE ranked_submissions
        9, 6, MATERIALIZE counts
        12, 9, SCAN json_each VIRTUAL TABLE INDEX 1:
        28, 6, SCAN submissions USING COVERING INDEX sqlite_autoindex_submissions_1
        30, 6, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        41, 6, SCAN counts LEFT-JOIN
        61, 6, USE TEMP B-TREE FOR ORDER BY
        73, 3, SCAN ranked_submissions
        84, 0, SCAN underlying_submissions
        86, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_prefer_distinct_random(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
        let metastate = MetaState::default();

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(Random),
        };
        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb, &metastate);

        let formatted_query = format(
            qb.sql().as_str(),
            &QueryParams::None,
            &FormatOptions::default(),
        );
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying_submissions AS MATERIALIZED (
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
              key,
              value as count
            FROM
              json_each(?)
          ),
          ranked_submissions AS MATERIALIZED (
            SELECT
              inner.submission_id
            FROM
              inner
              LEFT JOIN submissions_metadata sm ON inner.submission_id = sm.submission_id
              AND sm.metadata_key = ?
              LEFT JOIN counts ON sm.metadata_value = counts.key
            ORDER BY
              counts.count ASC NULLS FIRST
          )
          SELECT
            submission_id
          FROM
            ranked_submissions
        )
        SELECT
          chunks.*
        FROM
          underlying_submissions
          CROSS JOIN chunks
        WHERE
          chunks.submission_id = underlying_submissions.submission_id
          AND opsqueue_is_reserved(chunks.submission_id, chunk_index) = 0
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE underlying_submissions
        6, 3, MATERIALIZE ranked_submissions
        8, 6, CO-ROUTINE inner
        9, 8, COMPOUND QUERY
        10, 9, LEFT-MOST SUBQUERY
        13, 10, SEARCH submissions USING INDEX random_submissions_order (random_order>?)
        22, 9, UNION ALL
        25, 22, SEARCH submissions USING INDEX random_submissions_order (random_order<?)
        38, 6, MATERIALIZE counts
        41, 38, SCAN json_each VIRTUAL TABLE INDEX 1:
        56, 6, SCAN inner
        59, 6, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        71, 6, SCAN counts LEFT-JOIN
        91, 6, USE TEMP B-TREE FOR ORDER BY
        103, 3, SCAN ranked_submissions
        114, 0, SCAN underlying_submissions
        116, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_prefer_distinct_nested(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
        let metastate = MetaState::default();

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(PreferDistinct {
                meta_key: "priority".to_string(),
                underlying: Box::new(Oldest),
            }),
        };

        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb, &metastate);

        let formatted_query = format(
            qb.sql().as_str(),
            &QueryParams::None,
            &FormatOptions::default(),
        );
        insta::assert_snapshot!(formatted_query, @"
        WITH
        underlying_submissions AS MATERIALIZED (
          WITH
          inner AS NOT MATERIALIZED (
            WITH
            inner AS NOT MATERIALIZED (
              SELECT
                id AS submission_id
              FROM
                submissions
              ORDER BY
                id ASC
            ),
            counts AS (
              SELECT
                key,
                value as count
              FROM
                json_each(?)
            ),
            ranked_submissions AS MATERIALIZED (
              SELECT
                inner.submission_id
              FROM
                inner
                LEFT JOIN submissions_metadata sm ON inner.submission_id = sm.submission_id
                AND sm.metadata_key = ?
                LEFT JOIN counts ON sm.metadata_value = counts.key
              ORDER BY
                counts.count ASC NULLS FIRST
            )
            SELECT
              submission_id
            FROM
              ranked_submissions
          ),
          counts AS (
            SELECT
              key,
              value as count
            FROM
              json_each(?)
          ),
          ranked_submissions AS MATERIALIZED (
            SELECT
              inner.submission_id
            FROM
              inner
              LEFT JOIN submissions_metadata sm ON inner.submission_id = sm.submission_id
              AND sm.metadata_key = ?
              LEFT JOIN counts ON sm.metadata_value = counts.key
            ORDER BY
              counts.count ASC NULLS FIRST
          )
          SELECT
            submission_id
          FROM
            ranked_submissions
        )
        SELECT
          chunks.*
        FROM
          underlying_submissions
          CROSS JOIN chunks
        WHERE
          chunks.submission_id = underlying_submissions.submission_id
          AND opsqueue_is_reserved(chunks.submission_id, chunk_index) = 0
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE underlying_submissions
        6, 3, MATERIALIZE ranked_submissions
        9, 6, MATERIALIZE ranked_submissions
        12, 9, MATERIALIZE counts
        15, 12, SCAN json_each VIRTUAL TABLE INDEX 1:
        31, 9, SCAN submissions USING COVERING INDEX sqlite_autoindex_submissions_1
        33, 9, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        44, 9, SCAN counts LEFT-JOIN
        64, 9, USE TEMP B-TREE FOR ORDER BY
        75, 6, MATERIALIZE counts
        78, 75, SCAN json_each VIRTUAL TABLE INDEX 1:
        95, 6, SCAN ranked_submissions
        97, 6, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        109, 6, SCAN counts LEFT-JOIN
        129, 6, USE TEMP B-TREE FOR ORDER BY
        141, 3, SCAN ranked_submissions
        152, 0, SCAN underlying_submissions
        154, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
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
        register_reserved_lookup_noop(conn.get_inner()).await;
        let mut query_builder = QueryBuilder::default();
        let vals1: Vec<Chunk> = Strategy::Random
            .build_query(&mut query_builder, &MetaState::default())
            .build_query_as()
            .fetch(conn.get_inner())
            .try_collect()
            .await
            .unwrap();

        let mut query_builder = QueryBuilder::default();
        let vals2: Vec<Chunk> = Strategy::Random
            .build_query(&mut query_builder, &MetaState::default())
            .build_query_as()
            .fetch(conn.get_inner())
            .try_collect()
            .await
            .unwrap();

        assert!(vals1 != vals2);
    }

    /// Test for `PreferDistinct` that the next chunk should come from the
    /// submission where the associated metadata value (here `company_id`) has
    /// the *fewest* in-flight chunks in progress.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_prefer_distinct_picks_least_busy_company(pool: sqlx::SqlitePool) {
        use crate::consumer::dispatcher::metastate::MetaStateVal;

        let db_pools = crate::db::DBPools::from_test_pool(&pool);
        let mut conn = db_pools.writer_conn().await.unwrap();
        register_reserved_lookup_noop(conn.get_inner()).await;

        // Three companies, each with one submission of a few chunks.
        let company_ids: [MetaStateVal; 3] = [1, 2, 3];
        // For each inserted submission, keep track of the associated company.
        let mut company_id_per_submission = std::collections::HashMap::new();
        let chunks_per_company = 2;
        for company_id in company_ids {
            let strategic_metadata =
                StrategicMetadataMap::from_iter([("company_id".to_string(), company_id)]);
            let chunks: Vec<_> = (0..chunks_per_company)
                .map(|x| Some(x.to_string().into()))
                .collect();
            let submission_id = crate::common::submission::db::insert_submission_from_chunks(
                None,
                chunks,
                None,
                strategic_metadata,
                ChunkSize::default(),
                &mut conn,
            )
            .await
            .unwrap();
            company_id_per_submission.insert(submission_id, company_id);
        }

        // Company 1 is heavily in progress, 2 not at all, 3 a little.
        let metastate = MetaState::default();
        for _ in 0..chunks_per_company {
            metastate.increment("company_id", 1);
        }
        assert!(chunks_per_company > 1);
        metastate.increment("company_id", 3);

        let strategy = Strategy::PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(Strategy::Oldest),
        };
        let chunks: Vec<Chunk> = strategy
            .build_query(&mut QueryBuilder::default(), &metastate)
            .build_query_as()
            .fetch(conn.get_inner())
            .try_collect()
            .await
            .unwrap();

        let company_selection_order: Vec<_> = chunks
            .iter()
            .map(|chunk| company_id_per_submission[&chunk.submission_id])
            .collect();

        // Least-busy company first, busiest last.
        assert_eq!(company_selection_order, vec![2, 2, 3, 3, 1, 1]);
    }
}

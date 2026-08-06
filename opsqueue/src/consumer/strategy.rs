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

/// Iterator over the `meta_key`s of a chain of nested
/// [`Strategy::PreferDistinct`]. outermost first. Stops at the first
/// non-`PreferDistinct` strategy, which can afterwards be retrieved with
/// [`MetaKeysIter::take`].
pub struct MetaKeysIter<'a> {
    strategy: &'a Strategy,
}

impl<'a> MetaKeysIter<'a> {
    /// The first non-[`Strategy::PreferDistinct`] strategy in the chain.
    #[must_use]
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
    /// Iterate over the `meta_key`s of this chain of nested
    /// [`Strategy::PreferDistinct`], outermost first.
    #[must_use]
    pub fn meta_keys(&self) -> MetaKeysIter<'_> {
        MetaKeysIter { strategy: self }
    }
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
            PreferDistinct { .. } => {
                // Nested `PreferDistinct`s are flattened into a single query
                // level: rather than one CTE per level, we emit one `counts_N`
                // CTE per meta key and a single multi-key `ORDER BY`.
                let mut meta_keys_iter = self.meta_keys();
                let meta_keys: Vec<&str> = meta_keys_iter.by_ref().collect();
                let underlying = meta_keys_iter.take();

                // Unique submission IDs from the underlying strategy.
                let qb = qb.push("WITH inner AS NOT MATERIALIZED (");
                let qb = underlying.build_query_snippet_returning_submission_ids(qb);
                qb.push(")");
                // In-flight chunk count per submission, per meta key.
                //
                // The FFI call returns all counts as JSON in a single call.
                // The CROSS JOIN ON ensures the json_each is the outer loop,
                // and only performed once.
                for (i, meta_key) in meta_keys.iter().enumerate() {
                    qb.push(format!(
                        ", counts_{i} AS (
                            SELECT sm.submission_id, je.value AS count
                            FROM json_each(opsqueue_metadata_counts("
                    ));
                    qb.push_bind(*meta_key);
                    qb.push(
                        ")) je
                            CROSS JOIN submissions_metadata sm
                                ON sm.metadata_value = CAST(je.key AS INTEGER)
                            WHERE sm.metadata_key = ",
                    );
                    qb.push_bind(*meta_key);
                    qb.push(")");
                }
                // Submissions ranked by in-flight chunks. Submissions without a
                // value for a key get a NULL count and so are ranked first.
                qb.push(" SELECT inner.submission_id FROM inner");
                for i in 0..meta_keys.len() {
                    qb.push(format!(
                        " LEFT JOIN counts_{i} ON inner.submission_id = counts_{i}.submission_id"
                    ));
                }
                for i in 0..meta_keys.len() {
                    qb.push(if i == 0 { " ORDER BY " } else { ", " });
                    qb.push(format!("counts_{i}.count ASC NULLS FIRST"));
                }
                qb
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

    unsafe extern "C" fn sqlite_metadata_counts_lookup_noop(
        context: *mut ffi::sqlite3_context,
        _n_args: i32,
        _args: *mut *mut ffi::sqlite3_value,
    ) {
        unsafe {
            ffi::sqlite3_result_text(context, c"{}".as_ptr(), 2, ffi::SQLITE_TRANSIENT());
        };
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

        let function_name = b"opsqueue_metadata_counts\0";
        let rc = unsafe {
            ffi::sqlite3_create_function_v2(
                sqlite,
                function_name.as_ptr().cast(),
                1,
                ffi::SQLITE_UTF8,
                std::ptr::null_mut(),
                Some(sqlite_metadata_counts_lookup_noop),
                None,
                None,
                None,
            )
        };
        assert_eq!(
            rc,
            ffi::SQLITE_OK,
            "register opsqueue_metadata_counts failed"
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
          counts_0 AS (
            SELECT
              sm.submission_id,
              je.value AS count
            FROM
              json_each(opsqueue_metadata_counts(?)) je
              CROSS JOIN submissions_metadata sm ON sm.metadata_value = CAST(je.key AS INTEGER)
            WHERE
              sm.metadata_key = ?
          )
          SELECT
            inner.submission_id
          FROM
            inner
            LEFT JOIN counts_0 ON inner.submission_id = counts_0.submission_id
          ORDER BY
            counts_0.count ASC NULLS FIRST
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
        6, 3, MATERIALIZE counts_0
        10, 6, SCAN je VIRTUAL TABLE INDEX 1:
        15, 6, SEARCH sm USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=?)
        35, 3, SCAN submissions USING COVERING INDEX sqlite_autoindex_submissions_1
        41, 3, BLOOM FILTER ON counts_0 (submission_id=?)
        51, 3, SEARCH counts_0 USING AUTOMATIC COVERING INDEX (submission_id=?) LEFT-JOIN
        69, 3, USE TEMP B-TREE FOR ORDER BY
        81, 0, SCAN underlying_submission_ids
        83, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
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
          counts_0 AS (
            SELECT
              sm.submission_id,
              je.value AS count
            FROM
              json_each(opsqueue_metadata_counts(?)) je
              CROSS JOIN submissions_metadata sm ON sm.metadata_value = CAST(je.key AS INTEGER)
            WHERE
              sm.metadata_key = ?
          )
          SELECT
            inner.submission_id
          FROM
            inner
            LEFT JOIN counts_0 ON inner.submission_id = counts_0.submission_id
          ORDER BY
            counts_0.count ASC NULLS FIRST
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
        6, 3, MATERIALIZE counts_0
        10, 6, SCAN je VIRTUAL TABLE INDEX 1:
        15, 6, SEARCH sm USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=?)
        35, 3, SCAN submissions USING COVERING INDEX sqlite_autoindex_submissions_1
        41, 3, BLOOM FILTER ON counts_0 (submission_id=?)
        51, 3, SEARCH counts_0 USING AUTOMATIC COVERING INDEX (submission_id=?) LEFT-JOIN
        69, 3, USE TEMP B-TREE FOR ORDER BY
        81, 0, SCAN underlying_submission_ids
        83, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
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
          counts_0 AS (
            SELECT
              sm.submission_id,
              je.value AS count
            FROM
              json_each(opsqueue_metadata_counts(?)) je
              CROSS JOIN submissions_metadata sm ON sm.metadata_value = CAST(je.key AS INTEGER)
            WHERE
              sm.metadata_key = ?
          )
          SELECT
            inner.submission_id
          FROM
            inner
            LEFT JOIN counts_0 ON inner.submission_id = counts_0.submission_id
          ORDER BY
            counts_0.count ASC NULLS FIRST
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
        5, 3, CO-ROUTINE inner
        6, 5, COMPOUND QUERY
        7, 6, LEFT-MOST SUBQUERY
        10, 7, SEARCH submissions USING INDEX random_submissions_order (random_order>?)
        19, 6, UNION ALL
        22, 19, SEARCH submissions USING INDEX random_submissions_order (random_order<?)
        35, 3, MATERIALIZE counts_0
        39, 35, SCAN je VIRTUAL TABLE INDEX 1:
        44, 35, SEARCH sm USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=?)
        63, 3, SCAN inner
        70, 3, BLOOM FILTER ON counts_0 (submission_id=?)
        80, 3, SEARCH counts_0 USING AUTOMATIC COVERING INDEX (submission_id=?) LEFT-JOIN
        99, 3, USE TEMP B-TREE FOR ORDER BY
        111, 0, SCAN underlying_submission_ids
        113, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
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
          counts_0 AS (
            SELECT
              sm.submission_id,
              je.value AS count
            FROM
              json_each(opsqueue_metadata_counts(?)) je
              CROSS JOIN submissions_metadata sm ON sm.metadata_value = CAST(je.key AS INTEGER)
            WHERE
              sm.metadata_key = ?
          ),
          counts_1 AS (
            SELECT
              sm.submission_id,
              je.value AS count
            FROM
              json_each(opsqueue_metadata_counts(?)) je
              CROSS JOIN submissions_metadata sm ON sm.metadata_value = CAST(je.key AS INTEGER)
            WHERE
              sm.metadata_key = ?
          )
          SELECT
            inner.submission_id
          FROM
            inner
            LEFT JOIN counts_0 ON inner.submission_id = counts_0.submission_id
            LEFT JOIN counts_1 ON inner.submission_id = counts_1.submission_id
          ORDER BY
            counts_0.count ASC NULLS FIRST,
            counts_1.count ASC NULLS FIRST
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
        5, 3, CO-ROUTINE inner
        6, 5, COMPOUND QUERY
        7, 6, LEFT-MOST SUBQUERY
        10, 7, SEARCH submissions USING INDEX random_submissions_order (random_order>?)
        19, 6, UNION ALL
        22, 19, SEARCH submissions USING INDEX random_submissions_order (random_order<?)
        35, 3, MATERIALIZE counts_0
        39, 35, SCAN je VIRTUAL TABLE INDEX 1:
        44, 35, SEARCH sm USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=?)
        63, 3, MATERIALIZE counts_1
        67, 63, SCAN je VIRTUAL TABLE INDEX 1:
        72, 63, SEARCH sm USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=?)
        91, 3, SCAN inner
        98, 3, BLOOM FILTER ON counts_0 (submission_id=?)
        108, 3, SEARCH counts_0 USING AUTOMATIC COVERING INDEX (submission_id=?) LEFT-JOIN
        121, 3, BLOOM FILTER ON counts_1 (submission_id=?)
        131, 3, SEARCH counts_1 USING AUTOMATIC COVERING INDEX (submission_id=?) LEFT-JOIN
        155, 3, USE TEMP B-TREE FOR ORDER BY
        167, 0, SCAN underlying_submission_ids
        169, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
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

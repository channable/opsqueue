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
            Oldest => qb.push("SELECT * FROM chunks ORDER BY submission_id ASC"),
            Newest => qb.push("SELECT * FROM chunks ORDER BY submission_id DESC"),
            Random => Self::push_random_order_query(qb, "*", "chunks"),
            PreferDistinct { .. } => {
                // Unique submission IDs from the underlying strategy.
                let qb = qb.push("WITH underlying_submission_ids AS MATERIALIZED (");
                let qb = self.build_query_snippet_returning_submission_ids(qb, metastate);
                qb.push(") ");
                // In SQLite, <foo> CROSS JOIN <bar> ON/WHERE does NOT produce N
                // x M rows, it acts as an INNER JOIN but forces the query
                // planner to use '<foo>' as the outer loop, preserving the
                // underlying sort order.
                // c.f. https://sqlite.org/optoverview.html#manual_control_of_query_plans_using_cross_join
                qb.push(
                    " SELECT chunks.*
                        FROM underlying_submission_ids
                        CROSS JOIN chunks
                        ON chunks.submission_id = underlying_submission_ids.submission_id",
                )
            }
        }
    }

    fn build_query_snippet_returning_submission_ids<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<Sqlite>,
        metastate: &MetaState,
    ) -> &'a mut QueryBuilder<Sqlite> {
        use Strategy::{Newest, Oldest, PreferDistinct, Random};
        match self {
            Oldest => qb.push("SELECT id as submission_id FROM submissions ORDER BY id ASC"),
            Newest => qb.push("SELECT id as submission_id FROM submissions ORDER BY id DESC"),
            Random => Self::push_random_order_query(qb, "id as submission_id", "submissions"),
            PreferDistinct {
                meta_key,
                underlying,
            } => {
                // Unique submission IDs from the underlying strategy.
                let qb = qb.push("WITH inner AS NOT MATERIALIZED (");
                let qb = underlying.build_query_snippet_returning_submission_ids(qb, metastate);
                qb.push("),");
                // Count of in-flight chunks per submission.
                qb.push("counts AS (SELECT key, value FROM json_each(");
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
                    " LEFT JOIN counts c ON sm.metadata_value = c.key
                      ORDER BY c.value ASC NULLS FIRST
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
    ) -> &'a mut QueryBuilder<Sqlite> {
        let random_offset: u16 = rand::random();
        qb.push(format!(
            "SELECT {columns} FROM {table_name} WHERE random_order >= "
        ))
        .push_bind(random_offset)
        .push(format!(
            " UNION ALL SELECT {columns} FROM {table_name} WHERE random_order < "
        ))
        .push_bind(random_offset)
    }
}

#[cfg(feature = "server-logic")]
pub type ChunkStream<'a> = BoxStream<'a, Result<Chunk, sqlx::Error>>;

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {
    use crate::common::StrategicMetadataMap;
    use crate::common::chunk::ChunkSize;

    use super::*;
    use itertools::Itertools;
    use sqlformat::{FormatOptions, QueryParams, format};
    use sqlx::Row;
    use sqlx::{QueryBuilder, Sqlite, SqliteConnection};

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
        UNION ALL
        SELECT
          *
        FROM
          chunks
        WHERE
          random_order < ?
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_query(qb, &explained);
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        5, 2, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        22, 1, UNION ALL
        25, 22, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_prefer_distinct_oldest(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
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
              key,
              value
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
              LEFT JOIN counts c ON sm.metadata_value = c.key
            ORDER BY
              c.value ASC NULLS FIRST
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
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE underlying_submission_ids
        6, 3, MATERIALIZE ranked_submissions
        12, 6, SCAN submissions USING COVERING INDEX sqlite_autoindex_submissions_1
        14, 6, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        23, 6, SCAN json_each VIRTUAL TABLE INDEX 1: LEFT-JOIN
        46, 6, USE TEMP B-TREE FOR ORDER BY
        58, 3, SCAN ranked_submissions
        69, 0, SCAN underlying_submission_ids
        71, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_prefer_distinct_newest(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
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
              key,
              value
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
              LEFT JOIN counts c ON sm.metadata_value = c.key
            ORDER BY
              c.value ASC NULLS FIRST
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
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE underlying_submission_ids
        6, 3, MATERIALIZE ranked_submissions
        12, 6, SCAN submissions USING COVERING INDEX sqlite_autoindex_submissions_1
        14, 6, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        23, 6, SCAN json_each VIRTUAL TABLE INDEX 1: LEFT-JOIN
        46, 6, USE TEMP B-TREE FOR ORDER BY
        58, 3, SCAN ranked_submissions
        69, 0, SCAN underlying_submission_ids
        71, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_prefer_distinct_random(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
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
              key,
              value
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
              LEFT JOIN counts c ON sm.metadata_value = c.key
            ORDER BY
              c.value ASC NULLS FIRST
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
        40, 6, SCAN inner
        43, 6, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        53, 6, SCAN json_each VIRTUAL TABLE INDEX 1: LEFT-JOIN
        76, 6, USE TEMP B-TREE FOR ORDER BY
        88, 3, SCAN ranked_submissions
        99, 0, SCAN underlying_submission_ids
        101, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
        ");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_query_plan_prefer_distinct_nested(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        let metastate = MetaState::default();

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(PreferDistinct {
                meta_key: "priority".to_string(),
                underlying: Box::new(Random),
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
                key,
                value
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
                LEFT JOIN counts c ON sm.metadata_value = c.key
              ORDER BY
                c.value ASC NULLS FIRST
            )
            SELECT
              submission_id
            FROM
              ranked_submissions
          ),
          counts AS (
            SELECT
              key,
              value
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
              LEFT JOIN counts c ON sm.metadata_value = c.key
            ORDER BY
              c.value ASC NULLS FIRST
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
        43, 9, SCAN inner
        46, 9, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        56, 9, SCAN json_each VIRTUAL TABLE INDEX 1: LEFT-JOIN
        79, 9, USE TEMP B-TREE FOR ORDER BY
        94, 6, SCAN ranked_submissions
        96, 6, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        106, 6, SCAN json_each VIRTUAL TABLE INDEX 1: LEFT-JOIN
        129, 6, USE TEMP B-TREE FOR ORDER BY
        141, 3, SCAN ranked_submissions
        152, 0, SCAN underlying_submission_ids
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

        assert_ne!(vals1, vals2);
    }
}

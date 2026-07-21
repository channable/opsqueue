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
            Random => {
                let random_offset: u16 = rand::random();
                qb.push("SELECT * FROM chunks WHERE random_order >= ")
                    .push_bind(random_offset)
                    .push(" UNION ALL SELECT * FROM chunks WHERE random_order < ")
                    .push_bind(random_offset)
            }
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
                        WHERE chunks.submission_id = underlying_submissions.submission_id",
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
            Random => {
                panic!("Random underlying strategy not supported")
            }
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
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_query(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE ranked_submissions
        6, 3, MATERIALIZE counts
        9, 6, SCAN json_each VIRTUAL TABLE INDEX 1:
        25, 3, SCAN submissions USING COVERING INDEX sqlite_autoindex_submissions_1
        27, 3, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        38, 3, SCAN c LEFT-JOIN
        58, 3, USE TEMP B-TREE FOR ORDER BY
        70, 0, SCAN ranked_submissions
        72, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
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
        inner_company_id AS NOT MATERIALIZED (
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
        ),
        taken_company_id AS (
          SELECT
            *
          FROM
            submissions_metadata
          WHERE
            submissions_metadata.metadata_key = ?
            AND submissions_metadata.metadata_value IN (
              SELECT
                value
              FROM
                json_each()
            )
        )
        SELECT
          *
        FROM
          inner_company_id
        WHERE
          NOT EXISTS (
            SELECT
              1
            FROM
              taken_company_id
            WHERE
              inner_company_id.submission_id = taken_company_id.submission_id
          )
        UNION ALL
        SELECT
          *
        FROM
          inner_company_id
        WHERE
          EXISTS (
            SELECT
              1
            FROM
              taken_company_id
            WHERE
              inner_company_id.submission_id = taken_company_id.submission_id
          )
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_query(qb, &explained);
        insta::assert_snapshot!(explained, @"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        3, 2, COMPOUND QUERY
        4, 3, LEFT-MOST SUBQUERY
        7, 4, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        16, 4, CORRELATED SCALAR SUBQUERY 5
        20, 16, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        30, 16, LIST SUBQUERY 3
        32, 30, SCAN json_each VIRTUAL TABLE INDEX 0:
        58, 3, UNION ALL
        61, 58, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        71, 58, CORRELATED SCALAR SUBQUERY 5
        75, 71, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        85, 71, LIST SUBQUERY 3
        87, 85, SCAN json_each VIRTUAL TABLE INDEX 0:
        113, 3, UNION ALL
        114, 113, COMPOUND QUERY
        115, 114, LEFT-MOST SUBQUERY
        118, 115, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        127, 115, CORRELATED SCALAR SUBQUERY 7
        131, 127, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        141, 127, LIST SUBQUERY 3
        143, 141, SCAN json_each VIRTUAL TABLE INDEX 0:
        169, 114, UNION ALL
        172, 169, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        182, 169, CORRELATED SCALAR SUBQUERY 7
        186, 182, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        196, 182, LIST SUBQUERY 3
        198, 196, SCAN json_each VIRTUAL TABLE INDEX 0:
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
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_query(qb, &explained);
        insta::assert_snapshot!(explained, @"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        3, 2, COMPOUND QUERY
        4, 3, LEFT-MOST SUBQUERY
        5, 4, COMPOUND QUERY
        6, 5, LEFT-MOST SUBQUERY
        9, 6, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        18, 6, CORRELATED SCALAR SUBQUERY 5
        22, 18, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        32, 18, LIST SUBQUERY 3
        34, 32, SCAN json_each VIRTUAL TABLE INDEX 0:
        52, 6, CORRELATED SCALAR SUBQUERY 11
        56, 52, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        66, 52, LIST SUBQUERY 9
        68, 66, SCAN json_each VIRTUAL TABLE INDEX 0:
        94, 5, UNION ALL
        97, 94, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        107, 94, CORRELATED SCALAR SUBQUERY 5
        111, 107, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        121, 107, LIST SUBQUERY 3
        123, 121, SCAN json_each VIRTUAL TABLE INDEX 0:
        141, 94, CORRELATED SCALAR SUBQUERY 11
        145, 141, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        155, 141, LIST SUBQUERY 9
        157, 155, SCAN json_each VIRTUAL TABLE INDEX 0:
        183, 5, UNION ALL
        184, 183, COMPOUND QUERY
        185, 184, LEFT-MOST SUBQUERY
        188, 185, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        197, 185, CORRELATED SCALAR SUBQUERY 7
        201, 197, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        211, 197, LIST SUBQUERY 3
        213, 211, SCAN json_each VIRTUAL TABLE INDEX 0:
        231, 185, CORRELATED SCALAR SUBQUERY 11
        235, 231, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        245, 231, LIST SUBQUERY 9
        247, 245, SCAN json_each VIRTUAL TABLE INDEX 0:
        273, 184, UNION ALL
        276, 273, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        286, 273, CORRELATED SCALAR SUBQUERY 7
        290, 286, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        300, 286, LIST SUBQUERY 3
        302, 300, SCAN json_each VIRTUAL TABLE INDEX 0:
        320, 273, CORRELATED SCALAR SUBQUERY 11
        324, 320, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        334, 320, LIST SUBQUERY 9
        336, 334, SCAN json_each VIRTUAL TABLE INDEX 0:
        362, 184, UNION ALL
        363, 362, COMPOUND QUERY
        364, 363, LEFT-MOST SUBQUERY
        365, 364, COMPOUND QUERY
        366, 365, LEFT-MOST SUBQUERY
        369, 366, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        378, 366, CORRELATED SCALAR SUBQUERY 5
        382, 378, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        392, 378, LIST SUBQUERY 3
        394, 392, SCAN json_each VIRTUAL TABLE INDEX 0:
        412, 366, CORRELATED SCALAR SUBQUERY 13
        416, 412, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        426, 412, LIST SUBQUERY 9
        428, 426, SCAN json_each VIRTUAL TABLE INDEX 0:
        454, 365, UNION ALL
        457, 454, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        467, 454, CORRELATED SCALAR SUBQUERY 5
        471, 467, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        481, 467, LIST SUBQUERY 3
        483, 481, SCAN json_each VIRTUAL TABLE INDEX 0:
        501, 454, CORRELATED SCALAR SUBQUERY 13
        505, 501, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        515, 501, LIST SUBQUERY 9
        517, 515, SCAN json_each VIRTUAL TABLE INDEX 0:
        543, 365, UNION ALL
        544, 543, COMPOUND QUERY
        545, 544, LEFT-MOST SUBQUERY
        548, 545, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        557, 545, CORRELATED SCALAR SUBQUERY 7
        561, 557, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        571, 557, LIST SUBQUERY 3
        573, 571, SCAN json_each VIRTUAL TABLE INDEX 0:
        591, 545, CORRELATED SCALAR SUBQUERY 13
        595, 591, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        605, 591, LIST SUBQUERY 9
        607, 605, SCAN json_each VIRTUAL TABLE INDEX 0:
        633, 544, UNION ALL
        636, 633, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        646, 633, CORRELATED SCALAR SUBQUERY 7
        650, 646, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        660, 646, LIST SUBQUERY 3
        662, 660, SCAN json_each VIRTUAL TABLE INDEX 0:
        680, 633, CORRELATED SCALAR SUBQUERY 13
        684, 680, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        694, 680, LIST SUBQUERY 9
        696, 694, SCAN json_each VIRTUAL TABLE INDEX 0:
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

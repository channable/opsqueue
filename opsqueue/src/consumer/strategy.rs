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
        qb: &'a mut QueryBuilder<'a, Sqlite>,
        metastate: &MetaState,
    ) -> &'a mut QueryBuilder<'a, Sqlite> {
        let qb = self.build_query_snippet(qb, metastate, false);
        tracing::trace!("sql: {:?}", qb.sql());
        qb
    }

    fn build_query_snippet<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<'a, Sqlite>,
        metastate: &MetaState,
        only_submissions: bool,
    ) -> &'a mut QueryBuilder<'a, Sqlite> {
        use Strategy::*;

        match self {
            Oldest => {
                if only_submissions {
                    qb.push("SELECT id AS submission_id FROM submissions ORDER BY id ASC")
                } else {
                    qb.push("SELECT * FROM chunks ORDER BY submission_id ASC")
                }
            }
            Newest => {
                if only_submissions {
                    qb.push("SELECT id AS submission_id FROM submissions ORDER BY id DESC")
                } else {
                    qb.push("SELECT * FROM chunks ORDER BY submission_id DESC")
                }
            }
            Random => {
                if only_submissions {
                    // `submissions` has no `random_order` index (that lives on `chunks`),
                    // and this branch is only ever consumed by a `PreferDistinct` that
                    // re-sorts the (tiny) submission set anyway. So we simply order the
                    // submissions by the same Fibonacci-hash used for `chunks.random_order`
                    // (minus the per-chunk term), giving a stable, evenly-scattered shuffle.
                    qb.push(
                        "SELECT id AS submission_id FROM submissions \
                         ORDER BY (((id + (id >> 22)) % 65536) * 40503) % 65536",
                    )
                } else {
                    let random_offset: u16 = rand::random();
                    qb.push("SELECT * FROM chunks WHERE random_order >= ")
                        .push_bind(random_offset)
                        .push(" UNION ALL SELECT * FROM chunks WHERE random_order < ")
                        .push_bind(random_offset)
                }
            }
            PreferDistinct {
                meta_key,
                underlying,
            } => {
                // 1. Get ONLY the unique submission IDs from the underlying strategy.
                // Now targeting the tiny `submissions` table instead of deduplicating `chunks`!
                let qb = qb.push("WITH inner AS NOT MATERIALIZED (");
                let qb = underlying.build_query_snippet(qb, metastate, true);
                qb.push("),");

                // 2. Unpack the granular active counts into a virtual table.
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

                // 3. Rank the (few) in-flight submissions by how busy their metadata
                // value currently is: fewest chunks in progress first. A missing count
                // (NULL) means the value is not being worked at all, so it sorts first.
                //
                // This CTE MUST be MATERIALIZED: the sort is over the tiny `submissions`
                // set, and materializing it is what makes SQLite preserve the ranked
                // order when we walk it below. A NOT MATERIALIZED CTE would have its
                // ORDER BY discarded by the optimizer.
                qb.push(
                    "ranked_submissions AS MATERIALIZED (
                        SELECT inner.submission_id
                        FROM inner
                        LEFT JOIN submissions_metadata sm
                            ON inner.submission_id = sm.submission_id
                            AND sm.metadata_key = ",
                );
                qb.push_bind(meta_key);
                qb.push(
                    "
                        LEFT JOIN counts c
                            ON sm.metadata_value = c.key
                        ORDER BY c.value ASC NULLS FIRST
                    )",
                );

                // 4. Return either just the ranked submission IDs (when composed as an
                // underlying strategy) or the full chunks.
                //
                // The CROSS JOIN forces `ranked_submissions` as the outer loop, so its
                // ranked order is preserved and each submission's chunks are fetched by a
                // primary-key index seek. This keeps the read streamable (no chunk-level
                // scan or sort) and drops submissions that have no pending chunks.
                if only_submissions {
                    qb.push(" SELECT submission_id FROM ranked_submissions")
                } else {
                    qb.push(
                        " SELECT chunks.*
                          FROM ranked_submissions
                          CROSS JOIN chunks ON chunks.submission_id = ranked_submissions.submission_id",
                    )
                }
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
        assert!(
            !explained.contains("MATERIALIZED"),
            "Query should contain no materialization, but it did\n\nQuery: {query}\n\nPlan: \n\n {explained}"
        );
        assert!(
            !explained.contains("B-TREE"),
            "Query should contain no temporary B-tree construction, but it did.\n\nQuery: {query}\n\nPlan: \n\n{explained}"
        );
    }

    /// Weaker invariant used by `PreferDistinct`, which deliberately materializes and
    /// sorts the (tiny) set of in-flight submissions to rank them by how busy their
    /// metadata value currently is. That bounded sort is fine; what must never happen is
    /// a full scan or sort of the (potentially huge) `chunks` backlog. As long as `chunks`
    /// is only ever reached via an index seek, the read stays streamable and
    /// early-terminates after `limit` rows.
    fn assert_streaming_chunks(qb: &sqlx::QueryBuilder<'_, Sqlite>, explained: &str) {
        let query = qb.sql();
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
        let formatted_query = format(qb.sql(), &QueryParams::None, &options);
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
        let formatted_query = format(qb.sql(), &QueryParams::None, &options);
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

        let formatted_query = format(qb.sql(), &QueryParams::None, &FormatOptions::default());
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

        let formatted_query = format(qb.sql(), &QueryParams::None, &FormatOptions::default());
        insta::assert_snapshot!(formatted_query, @"
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
          chunks.*
        FROM
          ranked_submissions
          CROSS JOIN chunks ON chunks.submission_id = ranked_submissions.submission_id
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
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

        let formatted_query = format(qb.sql(), &QueryParams::None, &FormatOptions::default());
        insta::assert_snapshot!(formatted_query, @"
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
          chunks.*
        FROM
          ranked_submissions
          CROSS JOIN chunks ON chunks.submission_id = ranked_submissions.submission_id
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
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

        let formatted_query = format(qb.sql(), &QueryParams::None, &FormatOptions::default());
        insta::assert_snapshot!(formatted_query, @"
        WITH
        inner AS NOT MATERIALIZED (
          SELECT
            id AS submission_id
          FROM
            submissions
          ORDER BY
            (((id + (id >> 22)) % 65536) * 40503) % 65536
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
          chunks.*
        FROM
          ranked_submissions
          CROSS JOIN chunks ON chunks.submission_id = ranked_submissions.submission_id
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
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

        let formatted_query = format(qb.sql(), &QueryParams::None, &FormatOptions::default());
        insta::assert_snapshot!(formatted_query, @"
        WITH
        inner AS NOT MATERIALIZED (
          WITH
          inner AS NOT MATERIALIZED (
            SELECT
              id AS submission_id
            FROM
              submissions
            ORDER BY
              (((id + (id >> 22)) % 65536) * 40503) % 65536
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
          chunks.*
        FROM
          ranked_submissions
          CROSS JOIN chunks ON chunks.submission_id = ranked_submissions.submission_id
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_chunks(qb, &explained);
        insta::assert_snapshot!(explained, @"
        3, 0, MATERIALIZE ranked_submissions
        6, 3, MATERIALIZE ranked_submissions
        9, 6, MATERIALIZE counts
        12, 9, SCAN json_each VIRTUAL TABLE INDEX 1:
        28, 6, SCAN submissions USING COVERING INDEX sqlite_autoindex_submissions_1
        30, 6, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        41, 6, SCAN c LEFT-JOIN
        61, 6, USE TEMP B-TREE FOR ORDER BY
        72, 3, MATERIALIZE counts
        75, 72, SCAN json_each VIRTUAL TABLE INDEX 1:
        92, 3, SCAN ranked_submissions
        94, 3, SEARCH sm USING PRIMARY KEY (submission_id=? AND metadata_key=?) LEFT-JOIN
        106, 3, SCAN c LEFT-JOIN
        126, 3, USE TEMP B-TREE FOR ORDER BY
        138, 0, SCAN ranked_submissions
        140, 0, SEARCH chunks USING PRIMARY KEY (submission_id=?)
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
            .build_query(&mut query_builder, &Default::default())
            .build_query_as()
            .fetch(conn.get_inner())
            .try_collect()
            .await
            .unwrap();

        let mut query_builder = QueryBuilder::default();
        let vals2: Vec<Chunk> = Strategy::Random
            .build_query(&mut query_builder, &Default::default())
            .build_query_as()
            .fetch(conn.get_inner())
            .try_collect()
            .await
            .unwrap();

        assert!(vals1 != vals2)
    }

    /// Behavioural test for `PreferDistinct`: the next chunk should come from the
    /// metadata value (here `company_id`) that currently has the *fewest* chunks in
    /// progress. This guards the actual fairness ordering, which the query-plan
    /// snapshots above do not (a query can have the right shape yet emit the wrong
    /// order — as an earlier attempt did).
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_prefer_distinct_picks_least_busy_company(pool: sqlx::SqlitePool) {
        use crate::consumer::dispatcher::metastate::MetaStateVal;

        let db_pools = crate::db::DBPools::from_test_pool(&pool);
        let mut conn = db_pools.writer_conn().await.unwrap();

        // Three companies, each with a submission of a few chunks.
        let companies: [MetaStateVal; 3] = [100, 200, 300];
        let mut submission_of_company = std::collections::HashMap::new();
        for company in companies {
            let mut strategic_metadata = StrategicMetadataMap::default();
            strategic_metadata.insert("company_id".to_string(), company);
            let chunks: Vec<_> = (0..3).map(|x| Some(format!("{x}").into())).collect();
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
            submission_of_company.insert(submission_id, company);
        }

        // Company 100 is heavily in progress, 300 a little, 200 not at all.
        // Fairness should therefore prefer 200, then 300, then 100.
        let metastate = MetaState::default();
        for _ in 0..5 {
            metastate.increment("company_id", &100);
        }
        for _ in 0..2 {
            metastate.increment("company_id", &300);
        }

        let mut conn = db_pools.reader_conn().await.unwrap();
        let mut query_builder = QueryBuilder::default();
        let strategy = Strategy::PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(Strategy::Oldest),
        };
        let chunks: Vec<Chunk> = strategy
            .build_query(&mut query_builder, &metastate)
            .build_query_as()
            .fetch(conn.get_inner())
            .try_collect()
            .await
            .unwrap();

        let companies_in_order: Vec<_> = chunks
            .iter()
            .map(|chunk| submission_of_company[&chunk.submission_id])
            .dedup()
            .collect();

        // Least-busy company first, busiest last.
        assert_eq!(companies_in_order, vec![200, 300, 100]);
    }
}

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
        let qb = self.build_query_snippet(qb, metastate);
        tracing::trace!("sql: {:?}", qb.sql());
        qb
    }

    fn build_query_snippet<'a>(
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

            PreferDistinct {
                meta_key,
                underlying,
            } => {
                let qb = qb.push(format_args!("WITH inner_{meta_key} AS NOT MATERIALIZED ("));
                let qb = underlying.build_query_snippet(qb, metastate);
                qb.push(format_args!(
                    r"),
                taken_{meta_key} AS (
                    SELECT * FROM submissions_metadata
                    WHERE
                    submissions_metadata.metadata_key = ",
                ));
                qb.push_bind(meta_key);
                qb.push(
                    r" AND submissions_metadata.metadata_value IN (SELECT value FROM json_each(",
                );
                match metastate.get(meta_key) {
                    None => {
                        tracing::trace!("No metastatefield for key: {meta_key}");
                    }
                    Some(field) => {
                        let taken_values: Vec<_> = field.too_high_counts(1).collect();
                        let taken_values_string =
                            serde_json::to_string(&taken_values).expect("Always valid JSON");
                        tracing::trace!(
                            "Taken values that are left out of PreferDistinct: {taken_values_string:?}"
                        );
                        qb.push_bind(taken_values_string);
                    }
                }
                qb.push(format_args!("))
                )
                SELECT * FROM inner_{meta_key} WHERE NOT EXISTS (SELECT 1 FROM taken_{meta_key} WHERE inner_{meta_key}.submission_id = taken_{meta_key}.submission_id)
                UNION ALL
                SELECT * FROM inner_{meta_key} WHERE EXISTS (SELECT 1 FROM taken_{meta_key} WHERE inner_{meta_key}.submission_id = taken_{meta_key}.submission_id)
                "))
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
        inner_company_id AS NOT MATERIALIZED (
          SELECT
            *
          FROM
            chunks
          ORDER BY
            submission_id ASC
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
        5, 2, SCAN chunks
        8, 2, CORRELATED SCALAR SUBQUERY 4
        12, 8, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        22, 8, LIST SUBQUERY 2
        25, 22, SCAN json_each VIRTUAL TABLE INDEX 0:
        33, 22, CREATE BLOOM FILTER
        62, 1, UNION ALL
        65, 62, SCAN chunks
        68, 62, CORRELATED SCALAR SUBQUERY 6
        72, 68, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        80, 68, REUSE LIST SUBQUERY 2
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
        inner_company_id AS NOT MATERIALIZED (
          SELECT
            *
          FROM
            chunks
          ORDER BY
            submission_id DESC
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
        5, 2, SCAN chunks
        8, 2, CORRELATED SCALAR SUBQUERY 4
        12, 8, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        22, 8, LIST SUBQUERY 2
        25, 22, SCAN json_each VIRTUAL TABLE INDEX 0:
        33, 22, CREATE BLOOM FILTER
        62, 1, UNION ALL
        65, 62, SCAN chunks
        68, 62, CORRELATED SCALAR SUBQUERY 6
        72, 68, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        80, 68, REUSE LIST SUBQUERY 2
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
        33, 30, SCAN json_each VIRTUAL TABLE INDEX 0:
        41, 30, CREATE BLOOM FILTER
        62, 3, UNION ALL
        65, 62, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        75, 62, CORRELATED SCALAR SUBQUERY 5
        79, 75, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        87, 75, REUSE LIST SUBQUERY 3
        107, 3, UNION ALL
        108, 107, COMPOUND QUERY
        109, 108, LEFT-MOST SUBQUERY
        112, 109, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        121, 109, CORRELATED SCALAR SUBQUERY 7
        125, 121, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        133, 121, REUSE LIST SUBQUERY 3
        153, 108, UNION ALL
        156, 153, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        166, 153, CORRELATED SCALAR SUBQUERY 7
        170, 166, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        178, 166, REUSE LIST SUBQUERY 3
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
        inner_company_id AS NOT MATERIALIZED (
          WITH
          inner_priority AS NOT MATERIALIZED (
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
          taken_priority AS (
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
            inner_priority
          WHERE
            NOT EXISTS (
              SELECT
                1
              FROM
                taken_priority
              WHERE
                inner_priority.submission_id = taken_priority.submission_id
            )
          UNION ALL
          SELECT
            *
          FROM
            inner_priority
          WHERE
            EXISTS (
              SELECT
                1
              FROM
                taken_priority
              WHERE
                inner_priority.submission_id = taken_priority.submission_id
            )
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
        5, 4, COMPOUND QUERY
        6, 5, LEFT-MOST SUBQUERY
        9, 6, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        18, 6, CORRELATED SCALAR SUBQUERY 5
        22, 18, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        32, 18, LIST SUBQUERY 3
        35, 32, SCAN json_each VIRTUAL TABLE INDEX 0:
        43, 32, CREATE BLOOM FILTER
        56, 6, CORRELATED SCALAR SUBQUERY 11
        60, 56, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        70, 56, LIST SUBQUERY 9
        73, 70, SCAN json_each VIRTUAL TABLE INDEX 0:
        81, 70, CREATE BLOOM FILTER
        102, 5, UNION ALL
        105, 102, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        115, 102, CORRELATED SCALAR SUBQUERY 5
        119, 115, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        129, 115, LIST SUBQUERY 3
        132, 129, SCAN json_each VIRTUAL TABLE INDEX 0:
        140, 129, CREATE BLOOM FILTER
        153, 102, CORRELATED SCALAR SUBQUERY 11
        157, 153, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        167, 153, LIST SUBQUERY 9
        170, 167, SCAN json_each VIRTUAL TABLE INDEX 0:
        178, 167, CREATE BLOOM FILTER
        199, 5, UNION ALL
        200, 199, COMPOUND QUERY
        201, 200, LEFT-MOST SUBQUERY
        204, 201, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        213, 201, CORRELATED SCALAR SUBQUERY 7
        217, 213, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        227, 213, LIST SUBQUERY 3
        230, 227, SCAN json_each VIRTUAL TABLE INDEX 0:
        238, 227, CREATE BLOOM FILTER
        251, 201, CORRELATED SCALAR SUBQUERY 11
        255, 251, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        265, 251, LIST SUBQUERY 9
        268, 265, SCAN json_each VIRTUAL TABLE INDEX 0:
        276, 265, CREATE BLOOM FILTER
        297, 200, UNION ALL
        300, 297, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        310, 297, CORRELATED SCALAR SUBQUERY 7
        314, 310, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        324, 310, LIST SUBQUERY 3
        327, 324, SCAN json_each VIRTUAL TABLE INDEX 0:
        335, 324, CREATE BLOOM FILTER
        348, 297, CORRELATED SCALAR SUBQUERY 11
        352, 348, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        362, 348, LIST SUBQUERY 9
        365, 362, SCAN json_each VIRTUAL TABLE INDEX 0:
        373, 362, CREATE BLOOM FILTER
        394, 200, UNION ALL
        395, 394, COMPOUND QUERY
        396, 395, LEFT-MOST SUBQUERY
        397, 396, COMPOUND QUERY
        398, 397, LEFT-MOST SUBQUERY
        401, 398, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        410, 398, CORRELATED SCALAR SUBQUERY 5
        414, 410, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        424, 410, LIST SUBQUERY 3
        427, 424, SCAN json_each VIRTUAL TABLE INDEX 0:
        435, 424, CREATE BLOOM FILTER
        448, 398, CORRELATED SCALAR SUBQUERY 13
        452, 448, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        462, 448, LIST SUBQUERY 9
        465, 462, SCAN json_each VIRTUAL TABLE INDEX 0:
        473, 462, CREATE BLOOM FILTER
        494, 397, UNION ALL
        497, 494, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        507, 494, CORRELATED SCALAR SUBQUERY 5
        511, 507, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        521, 507, LIST SUBQUERY 3
        524, 521, SCAN json_each VIRTUAL TABLE INDEX 0:
        532, 521, CREATE BLOOM FILTER
        545, 494, CORRELATED SCALAR SUBQUERY 13
        549, 545, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        559, 545, LIST SUBQUERY 9
        562, 559, SCAN json_each VIRTUAL TABLE INDEX 0:
        570, 559, CREATE BLOOM FILTER
        591, 397, UNION ALL
        592, 591, COMPOUND QUERY
        593, 592, LEFT-MOST SUBQUERY
        596, 593, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        605, 593, CORRELATED SCALAR SUBQUERY 7
        609, 605, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        619, 605, LIST SUBQUERY 3
        622, 619, SCAN json_each VIRTUAL TABLE INDEX 0:
        630, 619, CREATE BLOOM FILTER
        643, 593, CORRELATED SCALAR SUBQUERY 13
        647, 643, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        657, 643, LIST SUBQUERY 9
        660, 657, SCAN json_each VIRTUAL TABLE INDEX 0:
        668, 657, CREATE BLOOM FILTER
        689, 592, UNION ALL
        692, 689, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        702, 689, CORRELATED SCALAR SUBQUERY 7
        706, 702, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        716, 702, LIST SUBQUERY 3
        719, 716, SCAN json_each VIRTUAL TABLE INDEX 0:
        727, 716, CREATE BLOOM FILTER
        740, 689, CORRELATED SCALAR SUBQUERY 13
        744, 740, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        754, 740, LIST SUBQUERY 9
        757, 754, SCAN json_each VIRTUAL TABLE INDEX 0:
        765, 754, CREATE BLOOM FILTER
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

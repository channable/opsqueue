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
        24, 22, SCAN json_each VIRTUAL TABLE INDEX 0:
        58, 1, UNION ALL
        61, 58, SCAN chunks
        64, 58, CORRELATED SCALAR SUBQUERY 6
        68, 64, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        78, 64, LIST SUBQUERY 2
        80, 78, SCAN json_each VIRTUAL TABLE INDEX 0:
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
        24, 22, SCAN json_each VIRTUAL TABLE INDEX 0:
        58, 1, UNION ALL
        61, 58, SCAN chunks
        64, 58, CORRELATED SCALAR SUBQUERY 6
        68, 64, SEARCH submissions_metadata USING PRIMARY KEY (submission_id=? AND metadata_key=?)
        78, 64, LIST SUBQUERY 2
        80, 78, SCAN json_each VIRTUAL TABLE INDEX 0:
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
            false,
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
}

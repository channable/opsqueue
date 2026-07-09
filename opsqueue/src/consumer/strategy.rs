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
        let qb = self.build_query_snippet(qb, metastate);
        let qb = self.build_sort_order_query_snippet(qb);
        tracing::trace!("sql: {:?}", qb.sql());
        qb
    }

    fn build_query_snippet<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<'a, Sqlite>,
        metastate: &MetaState,
    ) -> &'a mut QueryBuilder<'a, Sqlite> {
        use Strategy::*;
        match self {
            Oldest => qb.push("SELECT * FROM chunks WHERE submission_id NOT IN (SELECT id FROM submissions WHERE paused)"),
            Newest => qb.push("SELECT * FROM chunks WHERE submission_id NOT IN (SELECT id FROM submissions WHERE paused)"),
            Random => {
                let random_offset: u16 = rand::random();
                qb.push("SELECT * FROM chunks WHERE random_order >= ")
                    .push_bind(random_offset)
                    .push(" AND submission_id NOT IN (SELECT id FROM submissions WHERE paused) UNION ALL SELECT * FROM chunks WHERE random_order < ")
                    .push_bind(random_offset)
                    .push(" AND submission_id NOT IN (SELECT id FROM submissions WHERE paused)")
            }

            PreferDistinct {
                meta_key,
                underlying,
            } => {
                let qb = qb.push(format_args!("WITH inner_{meta_key} AS NOT MATERIALIZED ("));
                let qb = underlying.build_query_snippet(qb, metastate);
                let qb = underlying.build_sort_order_query_snippet(qb);
                qb.push(format_args!(
                    r#"),
                taken_{meta_key} AS (
                    SELECT * FROM submissions_metadata
                    WHERE
                    submissions_metadata.metadata_key = "#,
                ));
                qb.push_bind(meta_key);
                qb.push(
                    r#" AND submissions_metadata.metadata_value IN (SELECT value FROM json_each("#,
                );
                match metastate.get(meta_key) {
                    None => {
                        tracing::trace!("No metastatefield for key: {meta_key}");
                    }
                    Some(field) => {
                        let taken_values: Vec<_> = field.too_high_counts(1).collect();
                        let taken_values_string =
                            serde_json::to_string(&taken_values).expect("Always valid JSO");
                        tracing::trace!("Taken values that are left out of PreferDistinct: {taken_values_string:?}");
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

    fn build_sort_order_query_snippet<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<'a, Sqlite>,
    ) -> &'a mut QueryBuilder<'a, Sqlite> {
        use Strategy::*;
        match self {
            Oldest => qb.push("\nORDER BY submission_id ASC"),
            Newest => qb.push("\nORDER BY submission_id DESC"),
            Random => {
                // It is **very** important that we do not apply extra sorting here.
                // For the implementation of the 'cutting the deck' technique
                // we rely on the order being 'whatever comes out of the UNION ALL'
                qb
            }
            PreferDistinct { .. } => {
                // **no** change in sort order. PreferDistinct passes the sort order on to the inner strategies that it unions.
                qb
            }
        }
    }
}

#[cfg(feature = "server-logic")]
pub type ChunkStream<'a> = BoxStream<'a, Result<Chunk, sqlx::Error>>;

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {
    use crate::common::chunk::ChunkSize;
    use crate::common::StrategicMetadataMap;

    use super::*;
    use itertools::Itertools;
    use sqlx::Row;
    use sqlx::{QueryBuilder, Sqlite, SqliteConnection};

    async fn explain(
        qb: &mut sqlx::QueryBuilder<'_, Sqlite>,
        conn: &mut SqliteConnection,
    ) -> String {
        sqlx::raw_sql(&format!("EXPLAIN QUERY PLAN {}", qb.sql()))
            .fetch_all(&mut *conn)
            .await
            .unwrap_or_else(|_| panic!("Invalid query: \n{}\n", qb.sql()))
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
        let mut qb = QueryBuilder::new("");
        let metastate = MetaState::default();

        let qb = Strategy::Oldest.build_query(&mut qb, &metastate);
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        assert_eq!(
            explained,
            "3, 0, SCAN chunks\n8, 0, LIST SUBQUERY 1\n10, 8, SCAN submissions"
        );
    }

    #[sqlx::test]
    pub async fn test_query_plan_newest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let mut qb = QueryBuilder::new("");
        let metastate = MetaState::default();

        let qb = Strategy::Newest.build_query(&mut qb, &metastate);
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        assert_eq!(
            explained,
            "3, 0, SCAN chunks\n8, 0, LIST SUBQUERY 1\n10, 8, SCAN submissions"
        );
    }

    #[sqlx::test]
    pub async fn test_query_plan_random(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let metastate = MetaState::default();
        let mut qb = QueryBuilder::new("");

        let qb = Strategy::Random.build_query(&mut qb, &metastate);
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        5, 2, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        16, 2, LIST SUBQUERY 1
        18, 16, SCAN submissions
        45, 1, UNION ALL
        48, 45, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        60, 45, LIST SUBQUERY 3
        62, 60, SCAN submissions
        ");
    }

    #[sqlx::test]
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
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        5, 2, SCAN chunks
        10, 2, LIST SUBQUERY 1
        12, 10, SCAN submissions
        31, 2, CORRELATED SCALAR SUBQUERY 5
        35, 31, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        41, 31, LIST SUBQUERY 3
        43, 41, SCAN json_each VIRTUAL TABLE INDEX 0:
        86, 1, UNION ALL
        89, 86, SCAN chunks
        94, 86, LIST SUBQUERY 1
        96, 94, SCAN submissions
        115, 86, CORRELATED SCALAR SUBQUERY 7
        119, 115, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        125, 115, LIST SUBQUERY 3
        127, 125, SCAN json_each VIRTUAL TABLE INDEX 0:
        ");
    }

    #[sqlx::test]
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
        dbg!(qb.sql());
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        5, 2, SCAN chunks
        10, 2, LIST SUBQUERY 1
        12, 10, SCAN submissions
        31, 2, CORRELATED SCALAR SUBQUERY 5
        35, 31, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        41, 31, LIST SUBQUERY 3
        43, 41, SCAN json_each VIRTUAL TABLE INDEX 0:
        86, 1, UNION ALL
        89, 86, SCAN chunks
        94, 86, LIST SUBQUERY 1
        96, 94, SCAN submissions
        115, 86, CORRELATED SCALAR SUBQUERY 7
        119, 115, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        125, 115, LIST SUBQUERY 3
        127, 125, SCAN json_each VIRTUAL TABLE INDEX 0:
        ");
    }

    #[sqlx::test]
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
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        3, 2, COMPOUND QUERY
        4, 3, LEFT-MOST SUBQUERY
        7, 4, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        18, 4, LIST SUBQUERY 1
        20, 18, SCAN submissions
        39, 4, CORRELATED SCALAR SUBQUERY 7
        43, 39, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        49, 39, LIST SUBQUERY 5
        51, 49, SCAN json_each VIRTUAL TABLE INDEX 0:
        86, 3, UNION ALL
        89, 86, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        101, 86, LIST SUBQUERY 3
        103, 101, SCAN submissions
        122, 86, CORRELATED SCALAR SUBQUERY 7
        126, 122, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        132, 122, LIST SUBQUERY 5
        134, 132, SCAN json_each VIRTUAL TABLE INDEX 0:
        169, 3, UNION ALL
        170, 169, COMPOUND QUERY
        171, 170, LEFT-MOST SUBQUERY
        174, 171, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        185, 171, LIST SUBQUERY 1
        187, 185, SCAN submissions
        206, 171, CORRELATED SCALAR SUBQUERY 9
        210, 206, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        216, 206, LIST SUBQUERY 5
        218, 216, SCAN json_each VIRTUAL TABLE INDEX 0:
        253, 170, UNION ALL
        256, 253, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        268, 253, LIST SUBQUERY 3
        270, 268, SCAN submissions
        289, 253, CORRELATED SCALAR SUBQUERY 9
        293, 289, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        299, 289, LIST SUBQUERY 5
        301, 299, SCAN json_each VIRTUAL TABLE INDEX 0:
        ");
    }

    #[sqlx::test]
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
        let explained = explain(qb, &mut conn).await;
        assert!(
            !explained.contains("B-TREE"),
            "Query should contain no materialization, but it did: {explained}"
        );
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        3, 2, COMPOUND QUERY
        4, 3, LEFT-MOST SUBQUERY
        5, 4, COMPOUND QUERY
        6, 5, LEFT-MOST SUBQUERY
        9, 6, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        20, 6, LIST SUBQUERY 1
        22, 20, SCAN submissions
        41, 6, CORRELATED SCALAR SUBQUERY 7
        45, 41, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        51, 41, LIST SUBQUERY 5
        53, 51, SCAN json_each VIRTUAL TABLE INDEX 0:
        80, 6, CORRELATED SCALAR SUBQUERY 13
        84, 80, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        90, 80, LIST SUBQUERY 11
        92, 90, SCAN json_each VIRTUAL TABLE INDEX 0:
        127, 5, UNION ALL
        130, 127, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        142, 127, LIST SUBQUERY 3
        144, 142, SCAN submissions
        163, 127, CORRELATED SCALAR SUBQUERY 7
        167, 163, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        173, 163, LIST SUBQUERY 5
        175, 173, SCAN json_each VIRTUAL TABLE INDEX 0:
        202, 127, CORRELATED SCALAR SUBQUERY 13
        206, 202, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        212, 202, LIST SUBQUERY 11
        214, 212, SCAN json_each VIRTUAL TABLE INDEX 0:
        249, 5, UNION ALL
        250, 249, COMPOUND QUERY
        251, 250, LEFT-MOST SUBQUERY
        254, 251, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        265, 251, LIST SUBQUERY 1
        267, 265, SCAN submissions
        286, 251, CORRELATED SCALAR SUBQUERY 9
        290, 286, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        296, 286, LIST SUBQUERY 5
        298, 296, SCAN json_each VIRTUAL TABLE INDEX 0:
        325, 251, CORRELATED SCALAR SUBQUERY 13
        329, 325, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        335, 325, LIST SUBQUERY 11
        337, 335, SCAN json_each VIRTUAL TABLE INDEX 0:
        372, 250, UNION ALL
        375, 372, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        387, 372, LIST SUBQUERY 3
        389, 387, SCAN submissions
        408, 372, CORRELATED SCALAR SUBQUERY 9
        412, 408, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        418, 408, LIST SUBQUERY 5
        420, 418, SCAN json_each VIRTUAL TABLE INDEX 0:
        447, 372, CORRELATED SCALAR SUBQUERY 13
        451, 447, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        457, 447, LIST SUBQUERY 11
        459, 457, SCAN json_each VIRTUAL TABLE INDEX 0:
        494, 250, UNION ALL
        495, 494, COMPOUND QUERY
        496, 495, LEFT-MOST SUBQUERY
        497, 496, COMPOUND QUERY
        498, 497, LEFT-MOST SUBQUERY
        501, 498, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        512, 498, LIST SUBQUERY 1
        514, 512, SCAN submissions
        533, 498, CORRELATED SCALAR SUBQUERY 7
        537, 533, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        543, 533, LIST SUBQUERY 5
        545, 543, SCAN json_each VIRTUAL TABLE INDEX 0:
        572, 498, CORRELATED SCALAR SUBQUERY 15
        576, 572, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        582, 572, LIST SUBQUERY 11
        584, 582, SCAN json_each VIRTUAL TABLE INDEX 0:
        619, 497, UNION ALL
        622, 619, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        634, 619, LIST SUBQUERY 3
        636, 634, SCAN submissions
        655, 619, CORRELATED SCALAR SUBQUERY 7
        659, 655, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        665, 655, LIST SUBQUERY 5
        667, 665, SCAN json_each VIRTUAL TABLE INDEX 0:
        694, 619, CORRELATED SCALAR SUBQUERY 15
        698, 694, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        704, 694, LIST SUBQUERY 11
        706, 704, SCAN json_each VIRTUAL TABLE INDEX 0:
        741, 497, UNION ALL
        742, 741, COMPOUND QUERY
        743, 742, LEFT-MOST SUBQUERY
        746, 743, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        757, 743, LIST SUBQUERY 1
        759, 757, SCAN submissions
        778, 743, CORRELATED SCALAR SUBQUERY 9
        782, 778, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        788, 778, LIST SUBQUERY 5
        790, 788, SCAN json_each VIRTUAL TABLE INDEX 0:
        817, 743, CORRELATED SCALAR SUBQUERY 15
        821, 817, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        827, 817, LIST SUBQUERY 11
        829, 827, SCAN json_each VIRTUAL TABLE INDEX 0:
        864, 742, UNION ALL
        867, 864, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        879, 864, LIST SUBQUERY 3
        881, 879, SCAN submissions
        900, 864, CORRELATED SCALAR SUBQUERY 9
        904, 900, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        910, 900, LIST SUBQUERY 5
        912, 910, SCAN json_each VIRTUAL TABLE INDEX 0:
        939, 864, CORRELATED SCALAR SUBQUERY 15
        943, 939, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        949, 939, LIST SUBQUERY 11
        951, 949, SCAN json_each VIRTUAL TABLE INDEX 0:
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
            false,
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
}

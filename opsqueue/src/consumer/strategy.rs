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
            Oldest => qb.push("SELECT * FROM chunks"),
            Newest => qb.push("SELECT * FROM chunks"),
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
        assert_eq!(explained, "3, 0, SCAN chunks");
    }

    #[sqlx::test]
    pub async fn test_query_plan_newest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let mut qb = QueryBuilder::new("");
        let metastate = MetaState::default();

        let qb = Strategy::Newest.build_query(&mut qb, &metastate);
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        assert_eq!(explained, "3, 0, SCAN chunks");
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
        22, 1, UNION ALL
        25, 22, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
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
        8, 2, CORRELATED SCALAR SUBQUERY 4
        12, 8, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        18, 8, LIST SUBQUERY 2
        20, 18, SCAN json_each VIRTUAL TABLE INDEX 0:
        63, 1, UNION ALL
        66, 63, SCAN chunks
        69, 63, CORRELATED SCALAR SUBQUERY 6
        73, 69, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        79, 69, LIST SUBQUERY 2
        81, 79, SCAN json_each VIRTUAL TABLE INDEX 0:
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
        8, 2, CORRELATED SCALAR SUBQUERY 4
        12, 8, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        18, 8, LIST SUBQUERY 2
        20, 18, SCAN json_each VIRTUAL TABLE INDEX 0:
        63, 1, UNION ALL
        66, 63, SCAN chunks
        69, 63, CORRELATED SCALAR SUBQUERY 6
        73, 69, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        79, 69, LIST SUBQUERY 2
        81, 79, SCAN json_each VIRTUAL TABLE INDEX 0:
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
        16, 4, CORRELATED SCALAR SUBQUERY 5
        20, 16, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        26, 16, LIST SUBQUERY 3
        28, 26, SCAN json_each VIRTUAL TABLE INDEX 0:
        63, 3, UNION ALL
        66, 63, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        76, 63, CORRELATED SCALAR SUBQUERY 5
        80, 76, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        86, 76, LIST SUBQUERY 3
        88, 86, SCAN json_each VIRTUAL TABLE INDEX 0:
        123, 3, UNION ALL
        124, 123, COMPOUND QUERY
        125, 124, LEFT-MOST SUBQUERY
        128, 125, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        137, 125, CORRELATED SCALAR SUBQUERY 7
        141, 137, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        147, 137, LIST SUBQUERY 3
        149, 147, SCAN json_each VIRTUAL TABLE INDEX 0:
        184, 124, UNION ALL
        187, 184, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        197, 184, CORRELATED SCALAR SUBQUERY 7
        201, 197, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        207, 197, LIST SUBQUERY 3
        209, 207, SCAN json_each VIRTUAL TABLE INDEX 0:
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
        18, 6, CORRELATED SCALAR SUBQUERY 5
        22, 18, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        28, 18, LIST SUBQUERY 3
        30, 28, SCAN json_each VIRTUAL TABLE INDEX 0:
        57, 6, CORRELATED SCALAR SUBQUERY 11
        61, 57, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        67, 57, LIST SUBQUERY 9
        69, 67, SCAN json_each VIRTUAL TABLE INDEX 0:
        104, 5, UNION ALL
        107, 104, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        117, 104, CORRELATED SCALAR SUBQUERY 5
        121, 117, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        127, 117, LIST SUBQUERY 3
        129, 127, SCAN json_each VIRTUAL TABLE INDEX 0:
        156, 104, CORRELATED SCALAR SUBQUERY 11
        160, 156, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        166, 156, LIST SUBQUERY 9
        168, 166, SCAN json_each VIRTUAL TABLE INDEX 0:
        203, 5, UNION ALL
        204, 203, COMPOUND QUERY
        205, 204, LEFT-MOST SUBQUERY
        208, 205, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        217, 205, CORRELATED SCALAR SUBQUERY 7
        221, 217, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        227, 217, LIST SUBQUERY 3
        229, 227, SCAN json_each VIRTUAL TABLE INDEX 0:
        256, 205, CORRELATED SCALAR SUBQUERY 11
        260, 256, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        266, 256, LIST SUBQUERY 9
        268, 266, SCAN json_each VIRTUAL TABLE INDEX 0:
        303, 204, UNION ALL
        306, 303, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        316, 303, CORRELATED SCALAR SUBQUERY 7
        320, 316, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        326, 316, LIST SUBQUERY 3
        328, 326, SCAN json_each VIRTUAL TABLE INDEX 0:
        355, 303, CORRELATED SCALAR SUBQUERY 11
        359, 355, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        365, 355, LIST SUBQUERY 9
        367, 365, SCAN json_each VIRTUAL TABLE INDEX 0:
        402, 204, UNION ALL
        403, 402, COMPOUND QUERY
        404, 403, LEFT-MOST SUBQUERY
        405, 404, COMPOUND QUERY
        406, 405, LEFT-MOST SUBQUERY
        409, 406, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        418, 406, CORRELATED SCALAR SUBQUERY 5
        422, 418, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        428, 418, LIST SUBQUERY 3
        430, 428, SCAN json_each VIRTUAL TABLE INDEX 0:
        457, 406, CORRELATED SCALAR SUBQUERY 13
        461, 457, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        467, 457, LIST SUBQUERY 9
        469, 467, SCAN json_each VIRTUAL TABLE INDEX 0:
        504, 405, UNION ALL
        507, 504, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        517, 504, CORRELATED SCALAR SUBQUERY 5
        521, 517, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        527, 517, LIST SUBQUERY 3
        529, 527, SCAN json_each VIRTUAL TABLE INDEX 0:
        556, 504, CORRELATED SCALAR SUBQUERY 13
        560, 556, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        566, 556, LIST SUBQUERY 9
        568, 566, SCAN json_each VIRTUAL TABLE INDEX 0:
        603, 405, UNION ALL
        604, 603, COMPOUND QUERY
        605, 604, LEFT-MOST SUBQUERY
        608, 605, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        617, 605, CORRELATED SCALAR SUBQUERY 7
        621, 617, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        627, 617, LIST SUBQUERY 3
        629, 627, SCAN json_each VIRTUAL TABLE INDEX 0:
        656, 605, CORRELATED SCALAR SUBQUERY 13
        660, 656, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        666, 656, LIST SUBQUERY 9
        668, 666, SCAN json_each VIRTUAL TABLE INDEX 0:
        703, 604, UNION ALL
        706, 703, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        716, 703, CORRELATED SCALAR SUBQUERY 7
        720, 716, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        726, 716, LIST SUBQUERY 3
        728, 726, SCAN json_each VIRTUAL TABLE INDEX 0:
        755, 703, CORRELATED SCALAR SUBQUERY 13
        759, 755, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        765, 755, LIST SUBQUERY 9
        767, 765, SCAN json_each VIRTUAL TABLE INDEX 0:
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

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
            Random => qb.push("\nORDER BY random_order ASC"),
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
                format!("{}, {}, {}", id, parent, detail)
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
        1, 0, MERGE (UNION ALL)
        3, 1, LEFT
        7, 3, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        26, 1, RIGHT
        30, 26, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
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
        60, 1, UNION ALL
        63, 60, SCAN chunks
        66, 60, CORRELATED SCALAR SUBQUERY 6
        70, 66, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        76, 66, LIST SUBQUERY 2
        78, 76, SCAN json_each VIRTUAL TABLE INDEX 0:
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
        60, 1, UNION ALL
        63, 60, SCAN chunks
        66, 60, CORRELATED SCALAR SUBQUERY 6
        70, 66, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        76, 66, LIST SUBQUERY 2
        78, 76, SCAN json_each VIRTUAL TABLE INDEX 0:
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
        4, 2, CO-ROUTINE inner_company_id
        5, 4, MERGE (UNION ALL)
        7, 5, LEFT
        11, 7, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        30, 5, RIGHT
        34, 30, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        77, 2, SCAN inner_company_id
        81, 2, CORRELATED SCALAR SUBQUERY 5
        85, 81, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        91, 81, LIST SUBQUERY 3
        93, 91, SCAN json_each VIRTUAL TABLE INDEX 0:
        127, 1, UNION ALL
        129, 127, CO-ROUTINE inner_company_id
        130, 129, MERGE (UNION ALL)
        132, 130, LEFT
        136, 132, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        155, 130, RIGHT
        159, 155, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        202, 127, SCAN inner_company_id
        206, 127, CORRELATED SCALAR SUBQUERY 7
        210, 206, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        216, 206, LIST SUBQUERY 3
        218, 216, SCAN json_each VIRTUAL TABLE INDEX 0:
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
        6, 4, CO-ROUTINE inner_priority
        7, 6, MERGE (UNION ALL)
        9, 7, LEFT
        13, 9, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        32, 7, RIGHT
        36, 32, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        79, 4, SCAN inner_priority
        83, 4, CORRELATED SCALAR SUBQUERY 5
        87, 83, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        93, 83, LIST SUBQUERY 3
        95, 93, SCAN json_each VIRTUAL TABLE INDEX 0:
        123, 4, CORRELATED SCALAR SUBQUERY 11
        127, 123, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        133, 123, LIST SUBQUERY 9
        135, 133, SCAN json_each VIRTUAL TABLE INDEX 0:
        169, 3, UNION ALL
        171, 169, CO-ROUTINE inner_priority
        172, 171, MERGE (UNION ALL)
        174, 172, LEFT
        178, 174, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        197, 172, RIGHT
        201, 197, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        244, 169, SCAN inner_priority
        248, 169, CORRELATED SCALAR SUBQUERY 7
        252, 248, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        258, 248, LIST SUBQUERY 3
        260, 258, SCAN json_each VIRTUAL TABLE INDEX 0:
        288, 169, CORRELATED SCALAR SUBQUERY 11
        292, 288, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        298, 288, LIST SUBQUERY 9
        300, 298, SCAN json_each VIRTUAL TABLE INDEX 0:
        334, 3, UNION ALL
        335, 334, COMPOUND QUERY
        336, 335, LEFT-MOST SUBQUERY
        338, 336, CO-ROUTINE inner_priority
        339, 338, MERGE (UNION ALL)
        341, 339, LEFT
        345, 341, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        364, 339, RIGHT
        368, 364, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        411, 336, SCAN inner_priority
        415, 336, CORRELATED SCALAR SUBQUERY 5
        419, 415, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        425, 415, LIST SUBQUERY 3
        427, 425, SCAN json_each VIRTUAL TABLE INDEX 0:
        455, 336, CORRELATED SCALAR SUBQUERY 13
        459, 455, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        465, 455, LIST SUBQUERY 9
        467, 465, SCAN json_each VIRTUAL TABLE INDEX 0:
        501, 335, UNION ALL
        503, 501, CO-ROUTINE inner_priority
        504, 503, MERGE (UNION ALL)
        506, 504, LEFT
        510, 506, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        529, 504, RIGHT
        533, 529, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        576, 501, SCAN inner_priority
        580, 501, CORRELATED SCALAR SUBQUERY 7
        584, 580, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        590, 580, LIST SUBQUERY 3
        592, 590, SCAN json_each VIRTUAL TABLE INDEX 0:
        620, 501, CORRELATED SCALAR SUBQUERY 13
        624, 620, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        630, 620, LIST SUBQUERY 9
        632, 630, SCAN json_each VIRTUAL TABLE INDEX 0:
        ");
    }
}

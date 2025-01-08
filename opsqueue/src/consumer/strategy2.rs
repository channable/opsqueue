use futures::{future::Either, stream::BoxStream};
use serde::{Deserialize, Serialize};
use sqlx::{QueryBuilder, Sqlite, SqliteConnection};

use crate::common::chunk::Chunk;

type MetadataKey = String;
type MetadataValue = Either<String, i64>;
pub struct StrategyMetaState {
    strategic_metadata_counts: moka::sync::Cache<(MetadataKey, MetadataValue), u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BaseStrategy {
    Oldest,
    Newest,
    Random,
}

impl BaseStrategy {
    fn build_query_snippet<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<'a, Sqlite>,
    ) -> &'a mut QueryBuilder<'a, Sqlite> {
        use BaseStrategy::*;
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
        }
    }
    fn build_sort_order_query_snippet<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<'a, Sqlite>,
    ) -> &'a mut QueryBuilder<'a, Sqlite> {
        use BaseStrategy::*;
        match self {
            Oldest => qb.push("ORDER BY submission_id ASC"),
            Newest => qb.push("ORDER BY submission_id DESC"),
            Random => qb.push("ORDER BY random_order ASC"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    Base(BaseStrategy),
    PreferDistinct {
        meta_key: String,
        underlying: Box<Strategy>,
    },
}

impl Strategy {
    pub fn base_strategy(&self) -> &BaseStrategy {
        use Strategy::*;
        match self {
            Base(base) => base,
            PreferDistinct { underlying, .. } => underlying.base_strategy(),
        }
    }

    pub fn with_results_stream<'c, R>(
        &'c self,
        conn: &'c mut SqliteConnection,
        fun: impl FnOnce(ChunkStream<'_>) -> R,
    ) -> R {
        self.with_built_query(|qb| fun(qb.build_query_as().fetch(conn)))
    }

    pub fn with_built_query<R>(&self, fun: impl FnOnce(&mut QueryBuilder<'_, Sqlite>) -> R) -> R {
        let mut qb = QueryBuilder::new("");
        let qb = self.build_query(&mut qb);
        fun(qb)
    }

    pub fn build_query<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<'a, Sqlite>,
    ) -> &'a mut QueryBuilder<'a, Sqlite> {
        let qb = self.build_query_snippet(qb);
        qb.push("\n");
        let qb = self.base_strategy().build_sort_order_query_snippet(qb);
        qb
    }

    fn build_query_snippet<'a>(
        &'a self,
        qb: &'a mut QueryBuilder<'a, Sqlite>,
    ) -> &'a mut QueryBuilder<'a, Sqlite> {
        use Strategy::*;
        match self {
            Base(base) => base.build_query_snippet(qb),
            PreferDistinct {
                meta_key,
                underlying,
            } => {
                let taken_values = [1, 2, 3, 10, 20, 30, 100, 200, 1000, 12345]; // TODO: Fill with real value from reserver info
                let qb = qb.push(format_args!("WITH inner_{meta_key} AS ("));
                let qb = underlying.build_query_snippet(qb);
                qb.push(format_args!(
                    r#"),
                taken_{meta_key} AS (
                    SELECT * FROM submissions_metadata
                    WHERE
                    submissions_metadata.metadata_key = "#,
                ));
                qb.push_bind(meta_key);
                qb.push(r#" AND submissions_metadata.metadata_value IN ("#);
                // TODO: Refactor loop
                for index in 0..(taken_values.len()) {
                    if index > 0 {
                        qb.push(", ");
                    }
                    qb.push_bind(taken_values[index]);
                }

                qb.push(format_args!(")
                )
                SELECT * FROM inner_{meta_key} WHERE NOT EXISTS (SELECT 1 FROM taken_{meta_key} WHERE inner_{meta_key}.submission_id = taken_{meta_key}.submission_id)
                UNION ALL
                SELECT * FROM inner_{meta_key} WHERE EXISTS (SELECT 1 FROM taken_{meta_key} WHERE inner_{meta_key}.submission_id = taken_{meta_key}.submission_id)
                "))
            }
        }
    }
}

pub type ChunkStream<'a> = BoxStream<'a, Result<Chunk, sqlx::Error>>;

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {
    use super::*;
    use itertools::Itertools;
    use sqlx::Row;

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

    #[sqlx::test]
    pub async fn test_query_plan_oldest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let mut qb = QueryBuilder::new("");
        let qb = Strategy::Base(BaseStrategy::Oldest).build_query(&mut qb);
        println!("{}", qb.sql());
        let explained = explain(qb, &mut conn).await;
        assert_eq!(explained, "3, 0, SCAN chunks");
    }

    #[sqlx::test]
    pub async fn test_query_plan_newest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let mut qb = QueryBuilder::new("");
        let qb = Strategy::Base(BaseStrategy::Newest).build_query(&mut qb);
        println!("{}", qb.sql());
        let explained = explain(qb, &mut conn).await;
        assert_eq!(explained, "3, 0, SCAN chunks");
    }

    #[sqlx::test]
    pub async fn test_query_plan_random(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let mut qb = QueryBuilder::new("");

        let qb = Strategy::Base(BaseStrategy::Random).build_query(&mut qb);
        let explained = explain(qb, &mut conn).await;

        assert!(
            !explained.contains("B-TREE"),
            "Query should contain no materialization, but it did: {explained}"
        );
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
        use BaseStrategy::*;
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(Base(Oldest)),
        };
        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb);
        let explained = explain(qb, &mut conn).await;

        assert!(
            !explained.contains("B-TREE"),
            "Query should contain no materialization, but it did: {explained}"
        );
        insta::assert_snapshot!(explained, @r"
        1, 0, MERGE (UNION ALL)
        3, 1, LEFT
        6, 3, SCAN chunks
        9, 3, CORRELATED SCALAR SUBQUERY 3
        13, 9, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        83, 1, RIGHT
        86, 83, SCAN chunks
        89, 83, CORRELATED SCALAR SUBQUERY 5
        93, 89, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        ");
    }

    #[sqlx::test]
    pub async fn test_query_plan_prefer_distinct_newest(db: sqlx::SqlitePool) {
        use BaseStrategy::*;
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(Base(Newest)),
        };
        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb);
        let explained = explain(qb, &mut conn).await;

        assert!(
            !explained.contains("B-TREE"),
            "Query should contain no materialization, but it did: {explained}"
        );
        insta::assert_snapshot!(explained, @r"
        1, 0, MERGE (UNION ALL)
        3, 1, LEFT
        6, 3, SCAN chunks
        9, 3, CORRELATED SCALAR SUBQUERY 3
        13, 9, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        83, 1, RIGHT
        86, 83, SCAN chunks
        89, 83, CORRELATED SCALAR SUBQUERY 5
        93, 89, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        ");
    }

    #[sqlx::test]
    pub async fn test_query_plan_prefer_distinct_random(db: sqlx::SqlitePool) {
        use BaseStrategy::*;
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(Base(Random)),
        };
        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb);
        let explained = explain(qb, &mut conn).await;

        assert!(
            !explained.contains("B-TREE"),
            "Query should contain no materialization, but it did: {explained}"
        );
        insta::assert_snapshot!(explained, @r"
        1, 0, MERGE (UNION ALL)
        3, 1, LEFT
        4, 3, MERGE (UNION ALL)
        6, 4, LEFT
        10, 6, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        19, 6, CORRELATED SCALAR SUBQUERY 4
        23, 19, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        88, 4, RIGHT
        92, 88, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        102, 88, CORRELATED SCALAR SUBQUERY 4
        106, 102, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        195, 1, RIGHT
        196, 195, MERGE (UNION ALL)
        198, 196, LEFT
        202, 198, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        211, 198, CORRELATED SCALAR SUBQUERY 6
        215, 211, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        280, 196, RIGHT
        284, 280, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        294, 280, CORRELATED SCALAR SUBQUERY 6
        298, 294, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        ");
    }

    #[sqlx::test]
    pub async fn test_query_plan_prefer_distinct_nested(db: sqlx::SqlitePool) {
        use BaseStrategy::*;
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();

        let strategy = PreferDistinct {
            meta_key: "company_id".to_string(),
            underlying: Box::new(PreferDistinct {
                meta_key: "priority".to_string(),
                underlying: Box::new(Base(Random)),
            }),
        };

        let mut qb = QueryBuilder::new("");
        let qb = strategy.build_query(&mut qb);
        let explained = explain(qb, &mut conn).await;
        assert!(
            !explained.contains("B-TREE"),
            "Query should contain no materialization, but it did: {explained}"
        );
        insta::assert_snapshot!(explained, @r"
        1, 0, MERGE (UNION ALL)
        3, 1, LEFT
        4, 3, MERGE (UNION ALL)
        6, 4, LEFT
        7, 6, MERGE (UNION ALL)
        9, 7, LEFT
        13, 9, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        22, 9, CORRELATED SCALAR SUBQUERY 4
        26, 22, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        81, 9, CORRELATED SCALAR SUBQUERY 9
        85, 81, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        150, 7, RIGHT
        154, 150, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        164, 150, CORRELATED SCALAR SUBQUERY 4
        168, 164, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        223, 150, CORRELATED SCALAR SUBQUERY 9
        227, 223, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        316, 4, RIGHT
        317, 316, MERGE (UNION ALL)
        319, 317, LEFT
        323, 319, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        332, 319, CORRELATED SCALAR SUBQUERY 6
        336, 332, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        391, 319, CORRELATED SCALAR SUBQUERY 9
        395, 391, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        460, 317, RIGHT
        464, 460, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        474, 460, CORRELATED SCALAR SUBQUERY 6
        478, 474, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        533, 460, CORRELATED SCALAR SUBQUERY 9
        537, 533, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        650, 1, RIGHT
        651, 650, MERGE (UNION ALL)
        653, 651, LEFT
        654, 653, MERGE (UNION ALL)
        656, 654, LEFT
        660, 656, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        669, 656, CORRELATED SCALAR SUBQUERY 4
        673, 669, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        728, 656, CORRELATED SCALAR SUBQUERY 11
        732, 728, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        797, 654, RIGHT
        801, 797, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        811, 797, CORRELATED SCALAR SUBQUERY 4
        815, 811, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        870, 797, CORRELATED SCALAR SUBQUERY 11
        874, 870, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        963, 651, RIGHT
        964, 963, MERGE (UNION ALL)
        966, 964, LEFT
        970, 966, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        979, 966, CORRELATED SCALAR SUBQUERY 6
        983, 979, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        1038, 966, CORRELATED SCALAR SUBQUERY 11
        1042, 1038, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        1107, 964, RIGHT
        1111, 1107, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        1121, 1107, CORRELATED SCALAR SUBQUERY 6
        1125, 1121, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        1180, 1107, CORRELATED SCALAR SUBQUERY 11
        1184, 1180, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        ");
    }
}

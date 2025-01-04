use futures::stream::BoxStream;
use serde::{Serialize, Deserialize};
use sqlx::{QueryBuilder, Sqlite, SqliteConnection};

use crate::common::chunk::Chunk;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BaseStrategy {
    Oldest,
    Newest,
    Random,
}

impl BaseStrategy {
    fn build_query_snippet<'a>(&'a self, qb: &'a mut QueryBuilder<'a, Sqlite>) -> &'a mut QueryBuilder<'a, Sqlite> {
        qb.push("SELECT * FROM chunks")
    }
    fn build_sort_order_query_snippet<'a>(&'a self, qb: &'a mut QueryBuilder<'a, Sqlite>) -> &'a mut QueryBuilder<'a, Sqlite> {
        use BaseStrategy::*;
        match self {
            Oldest => qb.push("ORDER BY submission_id ASC"),
            Newest => qb.push ("ORDER BY submission_id DESC"),
            Random => qb.push("ORDER BY random_order ASC"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    Base(BaseStrategy),
    PreferDistinct{meta_key: String, underlying: Box<Strategy>},
}

impl Strategy {
    pub fn base_strategy(&self) -> &BaseStrategy {
        use Strategy ::*;
        match self {
            Base(base) => base,
            PreferDistinct{underlying, ..} => underlying.base_strategy(),
        }
    }

    pub fn with_results_stream<'c, R>(&'c self, conn: &'c mut SqliteConnection, fun: impl FnOnce(ChunkStream<'_>) -> R) -> R {
        self.with_built_query(|qb| {
            fun(qb.build_query_as().fetch(conn))
        })
    }

    pub fn with_built_query<R>(&self, fun: impl FnOnce(&mut QueryBuilder<'_, Sqlite>) -> R) -> R {
        let mut qb = QueryBuilder::new("");
        let qb = self.build_query(&mut qb);
        fun(qb)
    }

    pub fn build_query<'a>(&'a self, qb: &'a mut QueryBuilder<'a, Sqlite>) -> &'a mut QueryBuilder<'a, Sqlite> {
        let qb = self.build_query_snippet(qb);
        qb.push("\n");
        let qb = self.base_strategy().build_sort_order_query_snippet(qb);
        qb
    }

    fn build_query_snippet<'a>(&'a self, qb: &'a mut QueryBuilder<'a, Sqlite>) -> &'a mut QueryBuilder<'a, Sqlite>{
        use Strategy::*;
        match self {
            Base(base) => base.build_query_snippet(qb),
            PreferDistinct{meta_key, underlying} => {
                let taken_values = [1,2,3]; // TODO: Fill with real value from reserver info
                let qb = qb.push("WITH inner AS (");
                let qb = underlying.build_query_snippet(qb);
                qb.push(
                r#"),
                taken AS (
                    SELECT * FROM submissions_metadata
                    WHERE
                    submissions_metadata.metadata_key = "#);
                qb.push_bind(meta_key);
                qb.push(r#"AND submissions_metadata.metadata_value IN ("#);
                // TODO: Refactor loop
                for index in 0..(taken_values.len()) {
                    if index > 0 {
                        qb.push_bind(", ");
                    }
                    qb.push_bind(taken_values[index]);
                }

                qb.push(r#")
                )
                SELECT * FROM inner WHERE NOT EXISTS (SELECT 1 FROM taken WHERE inner.submission_id = taken.submission_id)
                UNION ALL
                SELECT * FROM inner WHERE EXISTS (SELECT 1 FROM taken WHERE inner.submission_id = taken.submission_id)
                "#)
            }
        }
    }
}


pub type ChunkStream<'a> = BoxStream<'a, Result<Chunk, sqlx::Error>>;


#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {
    use super::*;
    use sqlx::Row;
    use itertools::Itertools;

    async fn explain(qb: &mut sqlx::QueryBuilder<'_, Sqlite>, conn: &mut SqliteConnection) -> String {
        sqlx::raw_sql(&format!("EXPLAIN QUERY PLAN {}", qb.sql())).fetch_all(&mut *conn).await.unwrap()
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
        assert_eq!(explained, "AAA");
    }

    #[sqlx::test]
    pub async fn test_query_plan_newest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        let mut qb = QueryBuilder::new("");
        let qb = Strategy::Base(BaseStrategy::Newest).build_query(&mut qb);
        println!("{}", qb.sql());
        let explained = explain(qb, &mut conn).await;
        assert_eq!(explained, "AAA");
    }
}

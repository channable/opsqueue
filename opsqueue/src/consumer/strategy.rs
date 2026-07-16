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
            Oldest => qb
                .push("SELECT * FROM chunks")
                .push(" WHERE opsqueue_is_reserved(submission_id, chunk_index) = 0")
                .push(" ORDER BY submission_id ASC"),
            Newest => qb
                .push("SELECT * FROM chunks")
                .push(" WHERE opsqueue_is_reserved(submission_id, chunk_index) = 0")
                .push(" ORDER BY submission_id DESC"),
            Random => {
                let random_offset: u16 = rand::random();
                qb.push("SELECT * FROM chunks WHERE random_order >= ")
                    .push_bind(random_offset)
                    .push(" AND opsqueue_is_reserved(submission_id, chunk_index) = 0")
                    .push(" UNION ALL SELECT * FROM chunks WHERE random_order < ")
                    .push_bind(random_offset)
                    .push(" AND opsqueue_is_reserved(submission_id, chunk_index) = 0")
            }

            PreferDistinct {
                meta_key,
                underlying,
            } => {
                let qb = qb.push(format_args!("WITH inner_{meta_key} AS NOT MATERIALIZED ("));
                let qb = underlying.build_query_snippet(qb, metastate);
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
                            serde_json::to_string(&taken_values).expect("Always valid JSON");
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
}

#[cfg(feature = "server-logic")]
pub type ChunkStream<'a> = BoxStream<'a, Result<Chunk, sqlx::Error>>;

#[cfg(test)]
#[cfg(feature = "server-logic")]
pub mod test {
    use itertools::Itertools;
    use libsqlite3_sys as ffi;
    use sqlformat::{format, FormatOptions, QueryParams};
    use sqlx::Row;
    use sqlx::{QueryBuilder, Sqlite, SqliteConnection};

    use super::*;
    use crate::common::chunk::ChunkSize;
    use crate::common::StrategicMetadataMap;

    unsafe extern "C" fn sqlite_reserved_chunk_lookup_noop(
        context: *mut ffi::sqlite3_context,
        _n_args: i32,
        _args: *mut *mut ffi::sqlite3_value,
    ) {
        ffi::sqlite3_result_int(context, 0);
    }

    async fn register_reserved_lookup_noop(conn: &mut SqliteConnection) {
        let mut handle = conn.lock_handle().await.unwrap();
        let sqlite = handle.as_raw_handle().as_ptr();
        let function_name = b"opsqueue_is_reserved\0";
        let rc = unsafe {
            ffi::sqlite3_create_function_v2(
                sqlite,
                function_name.as_ptr().cast(),
                2,
                ffi::SQLITE_UTF8,
                std::ptr::null_mut(),
                Some(sqlite_reserved_chunk_lookup_noop),
                None,
                None,
                None,
            )
        };
        assert_eq!(rc, ffi::SQLITE_OK, "register opsqueue_is_reserved failed");
    }

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
        assert!(!explained.contains("MATERIALIZED"), "Query should contain no materialization, but it did\n\nQuery: {query}\n\nPlan: \n\n {explained}");
        assert!(!explained.contains("B-TREE"), "Query should contain no temporary B-tree construction, but it did.\n\nQuery: {query}\n\nPlan: \n\n{explained}");
    }

    #[sqlx::test]
    pub async fn test_query_plan_oldest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
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
        WHERE
          opsqueue_is_reserved(submission_id, chunk_index) = 0
        ORDER BY
          submission_id ASC
        ");
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        assert_eq!(explained, "3, 0, SCAN chunks");
    }

    #[sqlx::test]
    pub async fn test_query_plan_newest(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
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
        WHERE
          opsqueue_is_reserved(submission_id, chunk_index) = 0
        ORDER BY
          submission_id DESC
        ");
        let explained = explain(qb, &mut conn).await;

        assert_streaming_query(qb, &explained);
        assert_eq!(explained, "3, 0, SCAN chunks");
    }

    #[sqlx::test]
    pub async fn test_query_plan_random(db: sqlx::SqlitePool) {
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
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
          AND opsqueue_is_reserved(submission_id, chunk_index) = 0
        UNION ALL
        SELECT
          *
        FROM
          chunks
        WHERE
          random_order < ?
          AND opsqueue_is_reserved(submission_id, chunk_index) = 0
        ");

        let explained = explain(qb, &mut conn).await;
        assert_streaming_query(qb, &explained);
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        5, 2, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        26, 1, UNION ALL
        29, 26, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        ");
    }

    #[sqlx::test]
    pub async fn test_query_plan_prefer_distinct_oldest(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
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
        inner_company_id AS NOT MATERIALIZED (
          SELECT
            *
          FROM
            chunks
          WHERE
            opsqueue_is_reserved(submission_id, chunk_index) = 0
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
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        5, 2, SCAN chunks
        12, 2, CORRELATED SCALAR SUBQUERY 4
        16, 12, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        22, 12, LIST SUBQUERY 2
        24, 22, SCAN json_each VIRTUAL TABLE INDEX 0:
        67, 1, UNION ALL
        70, 67, SCAN chunks
        77, 67, CORRELATED SCALAR SUBQUERY 6
        81, 77, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        87, 77, LIST SUBQUERY 2
        89, 87, SCAN json_each VIRTUAL TABLE INDEX 0:
        ");
    }

    #[sqlx::test]
    pub async fn test_query_plan_prefer_distinct_newest(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
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
        inner_company_id AS NOT MATERIALIZED (
          SELECT
            *
          FROM
            chunks
          WHERE
            opsqueue_is_reserved(submission_id, chunk_index) = 0
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
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        5, 2, SCAN chunks
        12, 2, CORRELATED SCALAR SUBQUERY 4
        16, 12, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        22, 12, LIST SUBQUERY 2
        24, 22, SCAN json_each VIRTUAL TABLE INDEX 0:
        67, 1, UNION ALL
        70, 67, SCAN chunks
        77, 67, CORRELATED SCALAR SUBQUERY 6
        81, 77, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        87, 77, LIST SUBQUERY 2
        89, 87, SCAN json_each VIRTUAL TABLE INDEX 0:
        ");
    }

    #[sqlx::test]
    pub async fn test_query_plan_prefer_distinct_random(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
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
        inner_company_id AS NOT MATERIALIZED (
          SELECT
            *
          FROM
            chunks
          WHERE
            random_order >= ?
            AND opsqueue_is_reserved(submission_id, chunk_index) = 0
          UNION ALL
          SELECT
            *
          FROM
            chunks
          WHERE
            random_order < ?
            AND opsqueue_is_reserved(submission_id, chunk_index) = 0
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
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        3, 2, COMPOUND QUERY
        4, 3, LEFT-MOST SUBQUERY
        7, 4, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        20, 4, CORRELATED SCALAR SUBQUERY 5
        24, 20, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        30, 20, LIST SUBQUERY 3
        32, 30, SCAN json_each VIRTUAL TABLE INDEX 0:
        67, 3, UNION ALL
        70, 67, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        84, 67, CORRELATED SCALAR SUBQUERY 5
        88, 84, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        94, 84, LIST SUBQUERY 3
        96, 94, SCAN json_each VIRTUAL TABLE INDEX 0:
        131, 3, UNION ALL
        132, 131, COMPOUND QUERY
        133, 132, LEFT-MOST SUBQUERY
        136, 133, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        149, 133, CORRELATED SCALAR SUBQUERY 7
        153, 149, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        159, 149, LIST SUBQUERY 3
        161, 159, SCAN json_each VIRTUAL TABLE INDEX 0:
        196, 132, UNION ALL
        199, 196, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        213, 196, CORRELATED SCALAR SUBQUERY 7
        217, 213, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        223, 213, LIST SUBQUERY 3
        225, 223, SCAN json_each VIRTUAL TABLE INDEX 0:
        ");
    }

    #[sqlx::test]
    pub async fn test_query_plan_prefer_distinct_nested(db: sqlx::SqlitePool) {
        use Strategy::*;
        let mut conn = db.acquire().await.unwrap();
        register_reserved_lookup_noop(&mut conn).await;
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
        inner_company_id AS NOT MATERIALIZED (
          WITH
          inner_priority AS NOT MATERIALIZED (
            SELECT
              *
            FROM
              chunks
            WHERE
              random_order >= ?
              AND opsqueue_is_reserved(submission_id, chunk_index) = 0
            UNION ALL
            SELECT
              *
            FROM
              chunks
            WHERE
              random_order < ?
              AND opsqueue_is_reserved(submission_id, chunk_index) = 0
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
        insta::assert_snapshot!(explained, @r"
        1, 0, COMPOUND QUERY
        2, 1, LEFT-MOST SUBQUERY
        3, 2, COMPOUND QUERY
        4, 3, LEFT-MOST SUBQUERY
        5, 4, COMPOUND QUERY
        6, 5, LEFT-MOST SUBQUERY
        9, 6, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        22, 6, CORRELATED SCALAR SUBQUERY 5
        26, 22, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        32, 22, LIST SUBQUERY 3
        34, 32, SCAN json_each VIRTUAL TABLE INDEX 0:
        61, 6, CORRELATED SCALAR SUBQUERY 11
        65, 61, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        71, 61, LIST SUBQUERY 9
        73, 71, SCAN json_each VIRTUAL TABLE INDEX 0:
        108, 5, UNION ALL
        111, 108, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        125, 108, CORRELATED SCALAR SUBQUERY 5
        129, 125, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        135, 125, LIST SUBQUERY 3
        137, 135, SCAN json_each VIRTUAL TABLE INDEX 0:
        164, 108, CORRELATED SCALAR SUBQUERY 11
        168, 164, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        174, 164, LIST SUBQUERY 9
        176, 174, SCAN json_each VIRTUAL TABLE INDEX 0:
        211, 5, UNION ALL
        212, 211, COMPOUND QUERY
        213, 212, LEFT-MOST SUBQUERY
        216, 213, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        229, 213, CORRELATED SCALAR SUBQUERY 7
        233, 229, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        239, 229, LIST SUBQUERY 3
        241, 239, SCAN json_each VIRTUAL TABLE INDEX 0:
        268, 213, CORRELATED SCALAR SUBQUERY 11
        272, 268, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        278, 268, LIST SUBQUERY 9
        280, 278, SCAN json_each VIRTUAL TABLE INDEX 0:
        315, 212, UNION ALL
        318, 315, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        332, 315, CORRELATED SCALAR SUBQUERY 7
        336, 332, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        342, 332, LIST SUBQUERY 3
        344, 342, SCAN json_each VIRTUAL TABLE INDEX 0:
        371, 315, CORRELATED SCALAR SUBQUERY 11
        375, 371, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        381, 371, LIST SUBQUERY 9
        383, 381, SCAN json_each VIRTUAL TABLE INDEX 0:
        418, 212, UNION ALL
        419, 418, COMPOUND QUERY
        420, 419, LEFT-MOST SUBQUERY
        421, 420, COMPOUND QUERY
        422, 421, LEFT-MOST SUBQUERY
        425, 422, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        438, 422, CORRELATED SCALAR SUBQUERY 5
        442, 438, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        448, 438, LIST SUBQUERY 3
        450, 448, SCAN json_each VIRTUAL TABLE INDEX 0:
        477, 422, CORRELATED SCALAR SUBQUERY 13
        481, 477, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        487, 477, LIST SUBQUERY 9
        489, 487, SCAN json_each VIRTUAL TABLE INDEX 0:
        524, 421, UNION ALL
        527, 524, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        541, 524, CORRELATED SCALAR SUBQUERY 5
        545, 541, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        551, 541, LIST SUBQUERY 3
        553, 551, SCAN json_each VIRTUAL TABLE INDEX 0:
        580, 524, CORRELATED SCALAR SUBQUERY 13
        584, 580, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        590, 580, LIST SUBQUERY 9
        592, 590, SCAN json_each VIRTUAL TABLE INDEX 0:
        627, 421, UNION ALL
        628, 627, COMPOUND QUERY
        629, 628, LEFT-MOST SUBQUERY
        632, 629, SEARCH chunks USING INDEX random_chunks_order (random_order>?)
        645, 629, CORRELATED SCALAR SUBQUERY 7
        649, 645, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        655, 645, LIST SUBQUERY 3
        657, 655, SCAN json_each VIRTUAL TABLE INDEX 0:
        684, 629, CORRELATED SCALAR SUBQUERY 13
        688, 684, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        694, 684, LIST SUBQUERY 9
        696, 694, SCAN json_each VIRTUAL TABLE INDEX 0:
        731, 628, UNION ALL
        734, 731, SEARCH chunks USING INDEX random_chunks_order (random_order<?)
        748, 731, CORRELATED SCALAR SUBQUERY 7
        752, 748, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        758, 748, LIST SUBQUERY 3
        760, 758, SCAN json_each VIRTUAL TABLE INDEX 0:
        787, 731, CORRELATED SCALAR SUBQUERY 13
        791, 787, SEARCH submissions_metadata USING COVERING INDEX lookup_submission_by_metadata (metadata_key=? AND metadata_value=? AND submission_id=?)
        797, 787, LIST SUBQUERY 9
        799, 797, SCAN json_each VIRTUAL TABLE INDEX 0:
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
        register_reserved_lookup_noop(conn.get_inner()).await;
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

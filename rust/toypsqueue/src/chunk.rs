use std::ops::{Deref, DerefMut};

use chrono::{NaiveDateTime};
use sqlx::{query, Executor, QueryBuilder, Sqlite};
use sqlx::query_as;

pub type ChunkURI = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub submission_id: i64,
    pub id: i64,
    pub input_content: ChunkURI,
    pub retries: i64,
}


pub struct ChunkCompleted {
    pub submission_id: i64,
    pub id: i64,
    pub output_content: ChunkURI,
    pub completed_at: NaiveDateTime,
}

impl Chunk {
    pub fn new(submission_id: i64, chunk_index: u32, uri: ChunkURI) -> Self {
        Chunk {
            submission_id,
            id: chunk_index as i64,
            input_content: uri,
            retries: 0,
        }
    }
}

pub async fn insert_chunk(
    chunk: Chunk,
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<()> {
    query!(
        "INSERT INTO chunks (submission_id, id, input_content) VALUES ($1, $2, $3)",
        chunk.submission_id,
        chunk.id,
        chunk.input_content
    )
    .execute(conn)
    .await?;
    Ok(())
}

pub async fn complete_chunk(full_chunk_id: (i64, i64), output_content: Vec<u8>, conn: impl Executor<'_, Database = Sqlite>) -> sqlx::Result<()> {
    let now = chrono::prelude::Utc::now();
    query!("
    SAVEPOINT complete_chunk;

    INSERT INTO chunks_completed 
    (submission_id, id, output_content, completed_at) 
    SELECT submission_id, id, ?, julianday(?) FROM chunks WHERE chunks.submission_id = ? AND chunks.id = ?;

    DELETE FROM chunks WHERE chunks.submission_id = ? AND chunks.id = ? RETURNING *;

    RELEASE SAVEPOINT complete_chunk;
    ",
    output_content, 
    now,
    full_chunk_id.0, 
    full_chunk_id.1,
    full_chunk_id.0, 
    full_chunk_id.1,
    ).fetch_one(conn).await?;
    Ok(())
}

pub async fn fail_chunk(full_chunk_id: (i64, i64), failure: Vec<u8>, conn: impl Executor<'_, Database = Sqlite>) -> sqlx::Result<()> {
    let now = chrono::prelude::Utc::now();
    query!("
    SAVEPOINT fail_chunk;

    INSERT INTO chunks_failed
    (submission_id, id, input_content, failure, failed_at) 
    SELECT submission_id, id, input_content, ?, julianday(?) FROM chunks WHERE chunks.submission_id = ? AND chunks.id = ?;

    DELETE FROM chunks WHERE chunks.submission_id = ? AND chunks.id = ? RETURNING *;

    RELEASE SAVEPOINT fail_chunk;
    ", 
    failure, 
    now,
    full_chunk_id.0, 
    full_chunk_id.1, 
    full_chunk_id.0, 
    full_chunk_id.1, 
    ).fetch_one(conn).await?;
    Ok(())
}

pub async fn get_chunk(full_chunk_id: (i64, i64), conn: impl Executor<'_, Database = Sqlite>) -> sqlx::Result<Chunk> {
    query_as!(Chunk, "SELECT * FROM chunks WHERE submission_id =? AND id =?", full_chunk_id.0, full_chunk_id.1).fetch_one(conn).await
}

pub async fn get_chunk_completed(full_chunk_id: (i64, i64), conn: impl Executor<'_, Database = Sqlite>) -> sqlx::Result<ChunkCompleted> {
    query_as!(ChunkCompleted, "SELECT submission_id, id, output_content, completed_at FROM chunks_completed WHERE submission_id =? AND id =?", full_chunk_id.0, full_chunk_id.1).fetch_one(conn).await

}

pub async fn insert_many_chunks<Tx, Conn>(
    chunks: impl IntoIterator<Item = &Chunk>,
    mut conn: Tx,
) -> sqlx::Result<()>
where
    for<'a> &'a mut Conn: Executor<'a, Database = Sqlite>,
    Tx: Deref<Target = Conn> + DerefMut,
{
    let chunks_per_query = 1000;

    // let start = std::time::Instant::now();
    let mut iter = chunks.into_iter().peekable();
    while iter.peek().is_some() {
        let query_chunks = iter.by_ref().take(chunks_per_query);

        let mut query_builder: QueryBuilder<Sqlite> =
            QueryBuilder::new("INSERT INTO chunks (submission_id, id, uri) ");
        query_builder.push_values(query_chunks, |mut b, chunk| {
            b.push_bind(chunk.submission_id)
                .push_bind(chunk.id)
                .push_bind(chunk.input_content.clone());
        });
        let query = query_builder.build();

        query.execute(conn.deref_mut()).await?;
    }

    Ok(())
}

pub async fn select_oldest_chunks(db: impl sqlx::SqliteExecutor<'_>, count: u32) -> Vec<Chunk> {
    sqlx::query_as!(
        Chunk,
        "SELECT * FROM chunks ORDER BY submission_id ASC LIMIT $1",
        count
    )
    .fetch_all(db)
    .await
    .unwrap()
}

pub async fn select_newest_chunks(db: impl sqlx::SqliteExecutor<'_>, count: u32) -> Vec<Chunk> {
    sqlx::query_as!(
        Chunk,
        "SELECT * FROM chunks ORDER BY submission_id DESC LIMIT $1",
        count
    )
    .fetch_all(db)
    .await
    .unwrap()
}

pub async fn select_random_chunks(db: impl sqlx::SqliteExecutor<'_>, count: u32) -> Vec<Chunk> {
    // TODO: Document what we're doing here exactly
    let count_div10 = std::cmp::max(count / 10, 100);
    sqlx::query_as!(
        Chunk,
        "SELECT submission_id, id, input_content, retries FROM chunks JOIN
    (SELECT rowid as rid FROM chunks
        WHERE random() % $1 = 0  -- Reduce rowids by Nx
        LIMIT $2) AS srid
    ON chunks.rowid = srid.rid;",
        count_div10,
        count
    )
    .fetch_all(db)
    .await
    .unwrap()
}

pub async fn count_chunks(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks;").fetch_one(db).await.unwrap();
    Ok(count.count)
}

pub async fn count_chunks_completed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks_completed;").fetch_one(db).await.unwrap();
    Ok(count.count)
}

pub async fn count_chunks_failed(db: impl sqlx::SqliteExecutor<'_>) -> sqlx::Result<i32> {
    let count = sqlx::query!("SELECT COUNT(1) as count FROM chunks_failed;").fetch_one(db).await.unwrap();
    Ok(count.count)
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[sqlx::test]
    pub async fn test_insert_chunk(db: sqlx::SqlitePool) {
        let chunk = Chunk::new(1, 0, vec![1, 2, 3, 4, 5]);

        assert!(count_chunks(&db).await.unwrap() == 0);
        insert_chunk(chunk.clone(), &db).await.expect("Insert chunk failed");
        assert!(count_chunks(&db).await.unwrap() == 1);
    }

    #[sqlx::test]
    pub async fn test_get_chunk(db: sqlx::SqlitePool) {
        let chunk = Chunk::new(1, 0, vec![1, 2, 3, 4, 5]);
        insert_chunk(chunk.clone(), &db).await.expect("Insert chunk failed");

        let fetched_chunk = get_chunk((chunk.submission_id, chunk.id), &db).await.unwrap();
        assert!(chunk == fetched_chunk);
    }

    #[sqlx::test]
    pub async fn test_complete_chunk(db: sqlx::SqlitePool) {
        let chunk = Chunk::new(1, 0, vec![1, 2, 3, 4, 5]);

        insert_chunk(chunk.clone(), &db).await.expect("Insert chunk failed");
        complete_chunk((chunk.submission_id, chunk.id), vec![6,7,8,9], &db).await.expect("complete chunk failed");

        assert!(count_chunks(&db).await.unwrap() == 0);
        assert!(count_chunks_completed(&db).await.unwrap() == 1);
        assert!(count_chunks_failed(&db).await.unwrap() == 0);
    }


    #[sqlx::test]
    pub async fn test_fail_chunk(db: sqlx::SqlitePool) {
        let chunk = Chunk::new(1, 0, vec![1, 2, 3, 4, 5]);

        insert_chunk(chunk.clone(), &db).await.expect("Insert chunk failed");
        fail_chunk((chunk.submission_id, chunk.id), vec![6,7,8,9], &db).await.expect("Succeed chunk failed");

        assert!(count_chunks(&db).await.unwrap() == 0);
        assert!(count_chunks_completed(&db).await.unwrap() == 0);
        assert!(count_chunks_failed(&db).await.unwrap() == 1);
    }
}

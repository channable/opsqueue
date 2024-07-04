use std::ops::{Deref, DerefMut};

use sqlx::{query, Executor, Pool, QueryBuilder, Sqlite};

pub type ChunkURI = Vec<u8>;

#[derive(Debug, Clone)]
pub struct Chunk {
    pub submission_id: i64,
    pub id: i64,
    pub uri: ChunkURI,
}

impl Chunk {
    pub fn new(submission_id: i64, chunk_index: u32, uri: ChunkURI) -> Self {
        Chunk {
            submission_id,
            id: chunk_index as i64,
            uri,
        }
    }
}

pub async fn insert_chunk(
    chunk: Chunk,
    conn: impl Executor<'_, Database = Sqlite>,
) -> sqlx::Result<()> {
    query!(
        "INSERT INTO chunks (submission_id, id, uri) VALUES ($1, $2, $3)",
        chunk.submission_id,
        chunk.id,
        chunk.uri
    )
    .execute(conn)
    .await?;
    Ok(())
}

pub async fn insert_many_chunks<Tx, Conn>(chunks: &[Chunk], mut conn: Tx) -> sqlx::Result<()>
where
    for<'a> &'a mut Conn: Executor<'a, Database = Sqlite>,
    Tx: Deref<Target = Conn> + DerefMut,
{
    // let start = std::time::Instant::now();
    // NOTE: The following might be doable with itertools' Chunks to reduce copies further,
    // but combining this with Tokio async is a bit tricky.
    let chunks_per_query = 1000;
    for query_chunks in chunks.chunks(chunks_per_query) {
        // let inner_start = std::time::Instant::now();
        let mut query_builder: QueryBuilder<Sqlite> =
            QueryBuilder::new("INSERT INTO chunks (submission_id, id, uri) ");
        query_builder.push_values(query_chunks, |mut b, chunk| {
            b.push_bind(chunk.submission_id)
                .push_bind(chunk.id)
                .push_bind(chunk.uri.clone());
        });
        let query = query_builder.build();

        query.execute(conn.deref_mut()).await?;
        // println!(
        //     "Running one single query for {} chunks took {:?}",
        //     query_chunks.len(),
        //     inner_start.elapsed()
        // );
    }
    // println!(
    //     "Inserting {} chunks took {:?}",
    //     chunks.len(),
    //     start.elapsed()
    // );
    Ok(())
}

pub async fn select_oldest_chunks(db: impl sqlx::SqliteExecutor<'_>, count: u32) -> Vec<Chunk> {
    sqlx::query_as!(
        Chunk,
        "SELECT submission_id, id, uri FROM chunks ORDER BY submission_id ASC LIMIT $1",
        count
    )
    .fetch_all(db)
    .await
    .unwrap()
}

pub async fn select_newest_chunks(db: impl sqlx::SqliteExecutor<'_>, count: u32) -> Vec<Chunk> {
    sqlx::query_as!(
        Chunk,
        "SELECT submission_id, id, uri FROM chunks ORDER BY submission_id DESC LIMIT $1",
        count
    )
    .fetch_all(db)
    .await
    .unwrap()
}

pub async fn select_random_chunks(db: impl sqlx::SqliteExecutor<'_>, count: u32) -> Vec<Chunk> {
    let count_div10 = count / 2;
    sqlx::query_as!(
        Chunk,
        "SELECT submission_id, id, uri FROM chunks JOIN
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

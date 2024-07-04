use std::ops::{Deref, DerefMut};

use sqlx::{query, Executor, Pool, QueryBuilder, Sqlite};

pub type ChunkURI = Vec<u8>;

#[derive(Debug)]
pub struct Chunk {
    pub submission_id: i64,
    pub id: u32,
    pub uri: ChunkURI,
}

impl Chunk {
    pub fn new(submission_id: i64, chunk_index: u32, uri: ChunkURI) -> Self {
        Chunk {
            submission_id,
            id: chunk_index,
            uri,
        }
    }
}

pub async fn insert_chunk(chunk: Chunk, conn: impl Executor<'_, Database =Sqlite>) -> sqlx::Result<()> {
    query!("INSERT INTO chunks (submission_id, id, uri) VALUES ($1, $2, $3)", chunk.submission_id, chunk.id, chunk.uri)
    .execute(conn)
    .await?;
    Ok(())
}

pub async fn insert_many_chunks<Tx, Conn>(chunks: Vec<Chunk>, mut conn: Tx) -> sqlx::Result<()> 
where
  for<'a> &'a mut Conn: Executor<'a, Database = Sqlite>,
  Tx: Deref<Target = Conn> + DerefMut
{
    let start = std::time::Instant::now();
    // NOTE: The following might be doable with itertools' Chunks to reduce copies further, 
    // but combining this with Tokio async is a bit tricky.
    let chunks_per_query = 10000;
    for query_chunks in chunks.chunks(chunks_per_query) {

        let inner_start = std::time::Instant::now();
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new("INSERT INTO chunks (submission_id, id, uri) ");
        query_builder.push_values(query_chunks, |mut b, chunk| {
            b
            .push_bind(chunk.submission_id)
            .push_bind(chunk.id)
            .push_bind(chunk.uri.clone());
        });
        let query = query_builder.build();

        query.execute(conn.deref_mut()).await?;
        println!("Running one single query for {} chunks took {:?}", query_chunks.len(), inner_start.elapsed());
    }
    println!("Inserting {} chunks took {:?}", chunks.len(), start.elapsed());
    Ok(())
}

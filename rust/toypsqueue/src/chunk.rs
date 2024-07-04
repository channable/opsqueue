use sqlx::{Pool, query, Sqlite};

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
            submission_id: submission_id,
            id: chunk_index,
            uri,
        }
    }
}


pub async fn insert_chunk(chunk: &Chunk, pool: &Pool<Sqlite>) -> sqlx::Result<()> {
    query!("INSERT INTO chunks (submission_id, id, uri) VALUES ($1, $2, $3)", chunk.submission_id, chunk.id, chunk.uri)
    .execute(pool)
    .await?;
    Ok(())
}

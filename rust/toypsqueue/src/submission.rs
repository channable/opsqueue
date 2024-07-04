use sqlx::{Executor, Pool, Sqlite};

use crate::chunk::{Chunk, ChunkURI};

pub type Metadata = Vec<u8>;

static ID_GENERATOR: snowflaked::sync::Generator = snowflaked::sync::Generator::new(0);

#[derive(Debug)]
pub struct Submission {
    pub id: i64,
    pub chunks_total: u32,
    pub chunks_done: u32,
    pub metadata: Option<Metadata>,
}

impl Submission {
    pub fn new() -> Self {
        Submission {
            id: 0,
            chunks_total: 0,
            chunks_done: 0,
            metadata: None,
        }
    }

    fn generate_id() -> i64 {
        ID_GENERATOR.generate()
    }

    pub fn from_vec(chunks: Vec<ChunkURI>, metadata: Option<Metadata>) -> Option<(Submission, Vec<Chunk>)> {
        let submission_id = Self::generate_id();
        let len = chunks.len().try_into().ok()?;
        let submission = Submission {
            id: submission_id,
            chunks_total: len,
            chunks_done: 0,
            metadata,
        };
        let chunks = chunks
            .into_iter()
            .enumerate()
            .map(|(chunk_index, uri)| {
                // NOTE: we know that `len` fits in a u32 and therefore that the index fits in a u32 as well.
                let chunk_index = chunk_index as u32; 
                Chunk::new(submission_id, chunk_index, uri)
            })
            .collect();
        return Some((submission, chunks));
    }
}

pub async fn insert_submission(submission: Submission, conn: impl Executor<'_, Database =Sqlite>) -> sqlx::Result<()> {
    sqlx::query!("INSERT INTO submissions (id, chunks_total, chunks_done, metadata) VALUES ($1, $2, $3, $4)", submission.id, submission.chunks_total, submission.chunks_done, submission.metadata)
    .execute(conn)
    .await?;
    Ok(())
}

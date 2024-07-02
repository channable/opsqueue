use crate::chunk::{Chunk, ChunkURI};

pub type Metadata = Vec<u8>;


static ID_GENERATOR: snowflaked::sync::Generator = snowflaked::sync::Generator::new(0);

#[derive(Debug)]
pub struct Submission {
    pub id: u64,
    pub chunks_total: usize,
    pub chunks_done: usize,
    pub metadata: Option<Metadata>,
}

impl Submission {
    pub fn new() -> Self {
        Submission {id: 0, chunks_total: 0, chunks_done: 0, metadata: None}
    }

    fn generate_id() -> u64 {
        ID_GENERATOR.generate()
    }

    pub fn from_vec(chunks: Vec<ChunkURI>, metadata: Option<Metadata>) -> (Submission, Vec<Chunk>) {
        let submission_id = Self::generate_id();
        let submission = Submission {id: submission_id, chunks_total: chunks.len(), chunks_done: 0, metadata};
        let chunks = chunks.into_iter().enumerate().map(|(chunk_index, uri)| Chunk::new(submission_id, chunk_index, uri)).collect();
        return (submission, chunks);
    }
}

pub type ChunkURI = Vec<u8>;

#[derive(Debug)]
pub struct Chunk {
    pub submission_id: u64,
    pub id: usize,
    pub uri: ChunkURI,
}

impl Chunk {
    pub fn new(submission_id: u64, chunk_index: usize,  uri: ChunkURI) -> Self {
        Chunk {submission_id, id: chunk_index, uri}
    }
}

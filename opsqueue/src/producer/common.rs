use crate::common::StrategicMetadataMap;

use crate::common::{chunk, submission::Metadata};

/// A producer's request to create a new submission.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct InsertSubmission {
    pub chunk_contents: ChunkContents,
    pub metadata: Option<Metadata>,
    pub strategic_metadata: StrategicMetadataMap,
    pub chunk_size: Option<chunk::ChunkSize>,
}

/// Either embedded chunk contents or a reference to object storage.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ChunkContents {
    /// Use the `prefix` + the indexes 0..count
    /// to recover the contents of a chunk in the consumer.
    ///
    /// This is what you should use in production.
    SeeObjectStorage {
        prefix: String,
        count: chunk::ChunkIndex,
    },
    /// Directly pass the contents of each chunk in Opsqueue itself.
    ///
    /// NOTE: This is useful for small tests/examples,
    /// but significantly less scalable than using `SeeObjectStorage`.
    Direct { contents: Vec<chunk::Content> },
}

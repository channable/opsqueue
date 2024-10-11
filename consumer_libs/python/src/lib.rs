use std::sync::Arc;

use chrono::NaiveDateTime;
use pyo3::{create_exception, exceptions::PyException, prelude::*};

use opsqueue::common::{chunk, submission};
use opsqueue::consumer::client::Client as ActualClient;
use opsqueue::consumer::strategy;

create_exception!(opsqueue_consumer, ConsumerClientError, PyException);

#[pyclass]
#[derive(Debug, Clone)]
struct Client{client: ActualClient, runtime: Arc<tokio::runtime::Runtime>}

#[pymethods]
impl Client {
    #[new]
    pub fn new(address: &str) -> PyResult<Self> {
        let runtime = start_runtime();
        let client = runtime.block_on({
            ActualClient::new(address) 
        }).map_err(|e| ConsumerClientError::new_err(e.to_string()) )?;
        Ok(Client{client, runtime})
    }

    pub fn reserve_chunks(&self, max: usize, strategy: Strategy) -> PyResult<Vec<Chunk>> {
        self.runtime.block_on(self.client.reserve_chunks(max, strategy.into()))
        .map(|c| c.into_iter().map(Into::into).collect())
        .map_err(|e| ConsumerClientError::new_err(e.to_string()))
    }

    pub fn complete_chunk(&self, submission_id: SubmissionId, chunk_index: ChunkIndex, output_content: chunk::Content) -> PyResult<()> {
        let chunk_id = (submission_id.into(), chunk_index.into());
        self.runtime.block_on(self.client.complete_chunk(chunk_id, output_content)).map_err(|e| ConsumerClientError::new_err(e.to_string()))
    }
}

#[pyclass(frozen, get_all, eq, ord, hash)]
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct SubmissionId{pub id: i64}


#[pymethods]
impl SubmissionId {
    #[new]
    fn new(id: i64) -> Self {
        Self{id}
    }

    fn __repr__(&self) -> String {
        format!("SubmissionId(id={})", self.id)
    }
}

impl Into<submission::SubmissionId> for SubmissionId {
    fn into(self) -> submission::SubmissionId {
        submission::SubmissionId::from(self.id)
    }
}

impl Into<SubmissionId> for submission::SubmissionId {
    fn into(self) -> SubmissionId {
        SubmissionId { id: self.into() }
    }
}

#[pyclass(frozen, get_all, eq, ord, hash)]
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct ChunkIndex{pub id: i64}


#[pymethods]
impl ChunkIndex {
    #[new]
    fn new(id: i64) -> Self {
        Self{id}
    }

    fn __repr__(&self) -> String {
        format!("ChunkIndex(id={})", self.id)
    }
}

impl Into<chunk::ChunkIndex> for ChunkIndex {
    fn into(self) -> chunk::ChunkIndex {
        chunk::ChunkIndex::from(self.id)
    }
}

impl Into<ChunkIndex> for chunk::ChunkIndex {
    fn into(self) -> ChunkIndex {
        ChunkIndex { id: self.into() }
    }
}

#[pyclass(frozen, eq)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Strategy {
    Oldest,
    Newest,
}

impl From<strategy::Strategy> for Strategy {
    fn from(value: strategy::Strategy) -> Self {
        match value {
            strategy::Strategy::Oldest => Strategy::Oldest,
            strategy::Strategy::Newest => Strategy::Newest,
        }
    }
}

impl Into<strategy::Strategy> for Strategy {
    fn into(self) -> strategy::Strategy {
        match self {
            Strategy::Oldest => strategy::Strategy::Oldest,
            Strategy::Newest => strategy::Strategy::Newest,
        }
    }
}

#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub submission_id: SubmissionId,
    pub chunk_index: ChunkIndex,
    pub input_content: chunk::Content,
    pub retries: i64,
}

impl From<chunk::Chunk> for Chunk {
    fn from(value: chunk::Chunk) -> Self {
        Self {
            submission_id: value.submission_id.into(),
            chunk_index: value.chunk_index.into(),
            input_content: value.input_content,
            retries: value.retries,
        }
    }
}

#[pymethods]
impl Chunk {
    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pymethods]
impl Strategy {
    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pymethods]
impl SubmissionCompleted {
    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

impl From<opsqueue::common::submission::SubmissionCompleted> for SubmissionCompleted {
    fn from(value: opsqueue::common::submission::SubmissionCompleted) -> Self {
        Self {id: value.id, completed_at: value.completed_at, chunks_done: value.chunks_done, metadata: value.metadata}
    }
}

#[pymethods]
impl SubmissionFailed {
    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

impl From<opsqueue::common::submission::SubmissionFailed> for SubmissionFailed {
    fn from(value: opsqueue::common::submission::SubmissionFailed) -> Self {
        Self {id: value.id, failed_at: value.failed_at, chunks_total: value.chunks_total, metadata: value.metadata, failed_chunk_id: value.failed_chunk_id}
    }
}


#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionCompleted {
    pub id: i64,
    pub chunks_done: i64,
    pub metadata: Option<submission::Metadata>,
    pub completed_at: NaiveDateTime,
}

#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionFailed {
    pub id: i64,
    pub chunks_total: i64,
    pub metadata: Option<submission::Metadata>,
    pub failed_at: NaiveDateTime,
    pub failed_chunk_id: i64,
}

// #[pyclass(frozen, get_all)]
// #[derive(Debug, Clone)]
// struct InsertSubmission {
//     pub directory_uri: String,
//     pub chunk_count: u32,
//     pub metadata: Option<Metadata>,
// }

// #[pymethods]
// impl InsertSubmission {
//     #[new]
//     #[pyo3(signature = (directory_uri, chunk_count, metadata=None))]
//     fn new(directory_uri: String, chunk_count: u32, metadata: Option<Metadata>) -> Self {
//         Self{directory_uri, chunk_count, metadata}
//     }
// }

// impl Into<opsqueue::producer::server::InsertSubmission> for InsertSubmission {
//     fn into(self) -> opsqueue::producer::server::InsertSubmission {
//         opsqueue::producer::server::InsertSubmission {directory_uri: self.directory_uri, chunk_count: self.chunk_count, metadata: self.metadata}
//     }
// }

fn start_runtime() -> Arc<tokio::runtime::Runtime> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("Failed to create Tokio runtime in opsqueue Consumer client");
    Arc::new(runtime)
}

/// A Python module implemented in Rust.
#[pymodule]
fn opsqueue_consumer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Classes
    m.add_class::<Client>()?;
    m.add_class::<SubmissionId>()?;
    m.add_class::<ChunkIndex>()?;
    m.add_class::<Strategy>()?;

    // Exception classes
    m.add("ConsumerClientError", m.py().get_type_bound::<ConsumerClientError>())?;

    // Top-level functions
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}

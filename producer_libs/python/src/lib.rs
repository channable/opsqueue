use std::sync::Arc;

use chrono::NaiveDateTime;
use pyo3::{
    create_exception,
    exceptions::{PyException, PyTypeError},
    prelude::*,
};

use opsqueue::common::submission;
use opsqueue::producer::client::Client as ActualClient;

create_exception!(opsqueue_producer, ProducerClientError, PyException);

#[pyclass]
#[derive(Debug, Clone)]
struct Client {
    client: ActualClient,
    runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl Client {
    #[new]
    pub fn new(address: &str) -> Self {
        let runtime = start_runtime();
        let client = ActualClient::new(address);
        Client { client, runtime }
    }

    pub fn count_submissions(&self) -> PyResult<u32> {
        self.runtime
            .block_on(self.client.count_submissionns())
            .map_err(|e| PyTypeError::new_err(e.to_string()))
    }

    pub fn get_submission(&self, id: SubmissionId) -> PyResult<Option<SubmissionStatus>> {
        self.runtime
            .block_on(self.client.get_submission(id.into()))
            // .map(|opt| opt.map(|submission_status| format!("{submission_status:?}")))
            .map(|opt| opt.map(Into::into))
            .map_err(|e| ProducerClientError::new_err(e.to_string()))
    }

    #[pyo3(signature = (directory_uri, chunk_count, metadata=None))]
    pub fn insert_submission(
        &self,
        directory_uri: String,
        chunk_count: u32,
        metadata: Option<submission::Metadata>,
    ) -> PyResult<SubmissionId> {
        let submission = opsqueue::producer::server::InsertSubmission {
            directory_uri,
            chunk_count,
            metadata,
        };
        self.runtime
            .block_on(self.client.insert_submission(&submission))
            .map(Into::into)
            .map_err(|e| ProducerClientError::new_err(e.to_string()))
    }
}

#[pyclass(frozen, get_all, eq, ord, hash)]
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct SubmissionId {
    pub id: i64,
}

#[pymethods]
impl SubmissionId {
    #[new]
    fn new(id: i64) -> Self {
        Self { id }
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

#[pyclass(frozen)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionStatus {
    InProgress { submission: Submission },
    Completed { submission: SubmissionCompleted },
    Failed { submission: SubmissionFailed },
}

impl From<opsqueue::common::submission::SubmissionStatus> for SubmissionStatus {
    fn from(value: opsqueue::common::submission::SubmissionStatus) -> Self {
        use opsqueue::common::submission::SubmissionStatus::*;
        match value {
            InProgress(s) => SubmissionStatus::InProgress {
                submission: s.into(),
            },
            Completed(s) => SubmissionStatus::Completed {
                submission: s.into(),
            },
            Failed(s) => SubmissionStatus::Failed {
                submission: s.into(),
            },
        }
    }
}

#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    pub id: SubmissionId,
    pub chunks_total: i64,
    pub chunks_done: i64,
    pub metadata: Option<submission::Metadata>,
}

impl From<opsqueue::common::submission::Submission> for Submission {
    fn from(value: opsqueue::common::submission::Submission) -> Self {
        Self {
            id: value.id.into(),
            chunks_total: value.chunks_total,
            chunks_done: value.chunks_done,
            metadata: value.metadata,
        }
    }
}

#[pymethods]
impl Submission {
    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pymethods]
impl SubmissionStatus {
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
        Self {
            id: value.id,
            completed_at: value.completed_at,
            chunks_total: value.chunks_total,
            metadata: value.metadata,
        }
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
        Self {
            id: value.id,
            failed_at: value.failed_at,
            chunks_total: value.chunks_total,
            metadata: value.metadata,
            failed_chunk_id: value.failed_chunk_id,
        }
    }
}

#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionCompleted {
    pub id: i64,
    pub chunks_total: i64,
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
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime in opsqueue Producer client");
    Arc::new(runtime)
}

// /// Formats the sum of two numbers as string.
// #[pyfunction]
// fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
//     Ok((a + b).to_string())
// }

/// A Python module implemented in Rust.
#[pymodule]
fn opsqueue_producer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Classes
    m.add_class::<Client>()?;
    m.add_class::<SubmissionId>()?;

    // Exception classes
    m.add(
        "ProducerClientError",
        m.py().get_type_bound::<ProducerClientError>(),
    )?;

    // Top-level functions
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}

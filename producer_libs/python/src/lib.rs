use std::{future::IntoFuture, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use pyo3::{
    create_exception,
    exceptions::{PyException, PyTypeError},
    prelude::*, types::PyIterator,
};

use opsqueue::{common::{chunk, submission}, object_store::ChunkType, producer::server::ChunkContents};
use opsqueue::producer::client::Client as ProducerClient;
use futures::StreamExt;

create_exception!(opsqueue_producer, ProducerClientError, PyException);

// In development, check 10 times per second so we respond early to Ctrl+C
// But in production, only once per second so we don't fight as much over the GIL
#[cfg(debug_assertions)]
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(100);
#[cfg(not(debug_assertions))]
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_secs(1);

#[pyclass]
#[derive(Debug, Clone)]
struct Client {
    producer_client: ProducerClient,
    object_store_client: opsqueue::object_store::ObjectStoreClient,
    runtime: Arc<tokio::runtime::Runtime>,
}

fn maybe_wrap_error(e: anyhow::Error) -> PyErr {
    match e.downcast::<PyErr>() {
        Ok(py_err) => py_err,
        Err(other) => ProducerClientError::new_err(other.to_string()).into()
    }
}

#[pymethods]
impl Client {

    /// Create a new client instance.
    /// 
    /// :param address: The HTTP address where the opsqueue instance is running.
    /// 
    /// :param object_store_url: The URL used to upload/download objects from e.g. GCS.
    ///   use `file:///tmp/my/local/path` to use a local file when running small examples in development.
    ///   use `gs://bucket-name/path/inside/bucket` to connect to GCS in production.
    ///   Supports the formats listed here: https://docs.rs/object_store/0.11.1/object_store/enum.ObjectStoreScheme.html#method.parse
    ///   Note that other GCS settings are read from environment variables, using the steps outlined here: https://cloud.google.com/docs/authentication/application-default-credentials.
    #[new]
    pub fn new(address: &str, object_store_url: &str) -> PyResult<Self> {
        let runtime = start_runtime();
        let producer_client = ProducerClient::new(address);
        let object_store_client = 
            opsqueue::object_store::ObjectStoreClient::new(object_store_url)
            .map_err(maybe_wrap_error)?;
        Ok(Client { producer_client, object_store_client, runtime })
    }

    pub fn __repr__(&self) -> String {
        format!("<opsqueue_producer.Client(address={:?}, object_store_url={:?})>", self.producer_client.endpoint_url, self.object_store_client.url)
    }

    /// Counts the number of ongoing submissions in the queue.
    /// 
    /// Completed and failed submissions are not included in the count.
    pub fn count_submissions(&self, py: Python<'_>) -> PyResult<u32> {
        py.allow_threads(|| {
            self.block_unless_interrupted(self.producer_client.count_submissions())
                .map_err(|e| PyTypeError::new_err(e.to_string()))
        })
    }

    /// Retrieve the status (in progress, completed or failed) of a specific submission.
    /// 
    /// The returned SubmissionStatus object also includes the number of chunks finished so far,
    /// when the submission was started/completed/failed, etc.
    /// 
    /// This call does _not_ fetch the submission's chunk contents on its own.
    pub fn get_submission(&self, py: Python<'_>, id: SubmissionId) -> PyResult<Option<SubmissionStatus>> {
        py.allow_threads(||{
            self.block_unless_interrupted(self.producer_client.get_submission(id.into()))
                .map(|opt| opt.map(Into::into))
                .map_err(|e| ProducerClientError::new_err(e.to_string()))
        })
    }

    #[pyo3(signature = (chunk_contents, metadata=None))]
    pub fn insert_submission_direct(
        &self,
        py: Python<'_>,
        chunk_contents: Vec<chunk::Content>,
        metadata: Option<submission::Metadata>,
    ) -> PyResult<SubmissionId> {
        py.allow_threads(|| {
            let submission = opsqueue::producer::server::InsertSubmission2 {
                chunk_contents: ChunkContents::Direct { contents: chunk_contents },
                metadata,
            };
            self.block_unless_interrupted(self.producer_client.insert_submission(&submission))
                .map(Into::into)
                .map_err(|e| ProducerClientError::new_err(e.to_string()))
        })
    }

    #[pyo3(signature = (chunk_contents, metadata=None))]
    pub fn insert_submission(&self, py: Python<'_>, chunk_contents: Py<PyIterator>, metadata: Option<submission::Metadata>) -> PyResult<SubmissionId> {
        // This function is split into two parts.
        // For the upload to object storage, we need the GIL as we run the python iterator to completion.
        // For the second part, where we send the submission to the queue, we no longer need the GIL (and unlock it to allow logging later).
        py.allow_threads(|| {
            let prefix = uuid::Uuid::new_v4().to_string();
            let chunk_count = 
            Python::with_gil(|py| {
                self.block_unless_interrupted(async {
                let chunk_contents = chunk_contents.bind(py);
                let stream = futures::stream::iter(chunk_contents).map(|item| {
                    item.and_then(|item| item.extract())
                    .map_err(Into::into)
                });
                self.object_store_client.store_chunks(&prefix, ChunkType::Input, stream).await
                .map_err(maybe_wrap_error)
            })
        })?;

        self.block_unless_interrupted(async move {
            let submission = opsqueue::producer::server::InsertSubmission2 {
                chunk_contents: ChunkContents::SeeObjectStorage { prefix, count: chunk_count },
                metadata,
            };
            self.producer_client.insert_submission(&submission).await
            .map(Into::into)
            .map_err(maybe_wrap_error)
        })
    })
    }
}

// What follows are internal helper functions
// that are not available from Python
impl Client {
    fn block_unless_interrupted<T, E>(&self, future: impl IntoFuture<Output = Result<T, E>>) -> Result<T, E> 
    where
    E: From<PyErr>,
    {
        self.runtime.block_on(run_unless_interrupted(future))

    }

    // fn sleep_unless_interrupted<E>(&self, duration: Duration) -> Result<(), E> 
    // where
    //     E: From<PyErr>
    // {
    //     self.block_unless_interrupted(async {
    //         tokio::time::sleep(duration).await;
    //         Ok(())
    //     })
    // }
}

async fn run_unless_interrupted<T, E>(future: impl IntoFuture<Output = Result<T, E>>) -> Result<T, E>  
where
E: From<PyErr>,
{
    tokio::select! {
        res = future => res,
        py_err = check_signals_in_background() => Err(py_err.into())?,
    }
}

async fn check_signals_in_background() -> PyErr {
    loop {
        tokio::time::sleep(SIGNAL_CHECK_INTERVAL).await;
        if let Err(err) = Python::with_gil(|py| {py.check_signals()}) {
            return err;
        }
    }
}

#[pyclass(frozen, get_all, eq, ord, hash)]
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
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
        let submission_id: submission::SubmissionId = self.clone().into();
        format!("SubmissionId(id={}, timestamp={})", self.id, submission_id.timestamp())
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
        format!("Submission(id={0}, chunks_total={1}, chunks_done={2}, metadata={3:?})",
        self.id.__repr__(), self.chunks_total, self.chunks_done, self.metadata)
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
        format!("SubmissionCompleted(id={0}, chunks_total={1}, completed_at={2}, metadata={3:?})",
        self.id.__repr__(), self.chunks_total, self.completed_at, self.metadata)

    }
}

impl From<opsqueue::common::submission::SubmissionCompleted> for SubmissionCompleted {
    fn from(value: opsqueue::common::submission::SubmissionCompleted) -> Self {
        Self {
            id: value.id.into(),
            completed_at: value.completed_at,
            chunks_total: value.chunks_total,
            metadata: value.metadata,
        }
    }
}

#[pymethods]
impl SubmissionFailed {
    fn __repr__(&self) -> String {
        format!("SubmissionFailed(id={0}, chunks_total={1}, failed_at={2}, failed_chunk_id={3}, metadata={4:?})",
        self.id.__repr__(), self.chunks_total, self.failed_at, self.failed_chunk_id, self.metadata)
    }
}

impl From<opsqueue::common::submission::SubmissionFailed> for SubmissionFailed {
    fn from(value: opsqueue::common::submission::SubmissionFailed) -> Self {
        Self {
            id: value.id.into(),
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
    pub id: SubmissionId,
    pub chunks_total: i64,
    pub metadata: Option<submission::Metadata>,
    pub completed_at: DateTime<Utc>,
}

#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionFailed {
    pub id: SubmissionId,
    pub chunks_total: i64,
    pub metadata: Option<submission::Metadata>,
    pub failed_at: DateTime<Utc>,
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
    // We want Rust logs created by code called from this module
    // to be forwarded to Python's logging system
    pyo3_log::init();

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

use std::{future::IntoFuture, mem::MaybeUninit, pin::Pin, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use pyo3::{
    create_exception,
    exceptions::{PyException, PyTypeError},
    prelude::*, types::PyIterator,
};

use opsqueue::{common::{chunk, submission}, object_store::ChunkType, producer::server::ChunkContents};
use opsqueue::producer::client::Client as ProducerClient;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::sync::Mutex;

create_exception!(opsqueue_producer, ProducerClientError, PyException);

// In development, check 10 times per second so we respond early to Ctrl+C
// But in production, only once per second so we don't fight as much over the GIL
#[cfg(debug_assertions)]
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(100);
#[cfg(not(debug_assertions))]
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_secs(1);

const SUBMISSION_POLLING_INTERVAL: Duration = Duration::from_secs(1);

// NOTE: Client is reasonably cheap to clone, as most of its fields are behind Arcs.
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
        Err(other) => ProducerClientError::new_err(other.to_string())
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
            let submission = opsqueue::producer::server::InsertSubmission {
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
            let submission = opsqueue::producer::server::InsertSubmission {
                chunk_contents: ChunkContents::SeeObjectStorage { prefix, count: chunk_count },
                metadata,
            };
            self.producer_client.insert_submission(&submission).await
            .map(Into::into)
            .map_err(maybe_wrap_error)
        })
    })
    }

    pub fn stream_completed_submission(&self, py: Python<'_>, id: SubmissionId) -> PyResult<PyChunksIter> {
        py.allow_threads(|| {
            self.block_unless_interrupted(async move {
                match self.maybe_stream_completed_submission(id).await? {
                    None => Err(ProducerClientError::new_err("Submission not completed yet".to_string()))?,
                    Some(py_iter) => {
                        Ok(py_iter)
                    }
                }
            })
        })
    }

    #[pyo3(signature = (chunk_contents, metadata=None))]
    pub fn run_submission(&self, py: Python<'_>, chunk_contents: Py<PyIterator>, metadata: Option<submission::Metadata>) -> PyResult<PyChunksIter> {
        let submission_id = self.insert_submission(py, chunk_contents, metadata)?;
        py.allow_threads(||{
            self.block_unless_interrupted(async move {
                loop {
                    if let Some(py_stream) = self.maybe_stream_completed_submission(submission_id).await? {
                        return Ok(py_stream)
                    }
                    tokio::time::sleep(SUBMISSION_POLLING_INTERVAL).await;
                }
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

    async fn maybe_stream_completed_submission(&self, id: SubmissionId) -> PyResult<Option<PyChunksIter>> {
        match self.producer_client.get_submission(id.into()).await.map_err(maybe_wrap_error)? {
            Some(submission::SubmissionStatus::Completed(submission)) => {
                let prefix = submission.prefix.unwrap_or_default();
                let py_chunks_iter = PyChunksIter::new(self.clone(), prefix, submission.chunks_total).await;

                Ok(Some(py_chunks_iter))
            }
            _ => Ok(None)
        }
    }
}

type PinfulStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;

// TODO: This is ugly and painful.
// At the very least, we should double-check the soundness with miri.
#[pyclass]
struct PyChunksIter {
    stream: MaybeUninit<Mutex<PinfulStream<PyResult<Vec<u8>>>>>,
    // SAFETY:
    // The following fields _have_ to be boxed so they won't move in memory once the struct itself moves.
    // They also _have_ to be after the `stream` field, ensuring they are dropped _after_ stream is dropped.
    self_borrows: Box<(String, Client)>,
}

impl PyChunksIter {
    pub (crate) async fn new(client: Client, prefix: String, chunks_total: i64) -> Self {
        let self_borrows = Box::new((prefix, client));
        let mut me = Self {self_borrows, stream: MaybeUninit::uninit() };

        let stream = me.self_borrows.1.object_store_client.retrieve_chunks(&me.self_borrows.0, chunks_total, ChunkType::Output).await;
        let stream = stream.map_err(maybe_wrap_error);
        // SAFETY:
        // Welcome in self-referential struct land.
        //
        // We have to transmute the too-short lifetime to a fake 'static lifetime,
        // to convince the compiler that `self.stream` will never outlive the passed `client` and `prefix`.
        // As long as we indeed pass `self.client` and `self.prefix` and never take the `stream` field out of the PyChunksIter struct
        // after creation, this is sound.
        //
        // We have to resort to a self-referential struct because lifetimes cannot cross over to Python,
        // so we have to pack the stream with all of its dependencies.
        let stream: Pin<Box<dyn Stream<Item = PyResult<Vec<u8>>> + Send>> = Box::pin(stream);
        let stream: Pin<Box<dyn Stream<Item = PyResult<Vec<u8>>> + Send>> = unsafe { std::mem::transmute(stream) };
        me.stream = MaybeUninit::new(Mutex::new(stream));

        me
    }
}

#[pymethods]
impl PyChunksIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRefMut<'_, Self>) -> Option<PyResult<VecAsPyBytes>> {
        slf.self_borrows.1.runtime.block_on(async {
            // SAFETY: The MaybeUninit is always initialized once `PyChunksIter::new` returns.
            let mut stream = unsafe { slf.stream.assume_init_ref() }.lock().await;
            stream.next().await.map(|r| r.map(VecAsPyBytes))
        })
        // slf.iter.next().map(|result| result.map(VecAsPyBytes))
    }
}




async fn run_unless_interrupted<T, E>(future: impl IntoFuture<Output = Result<T, E>>) -> Result<T, E>
where
E: From<PyErr>,
{
    tokio::select! {
        res = future => res,
        py_err = check_signals_in_background() => Err(py_err)?,
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
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
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
        let submission_id: submission::SubmissionId = (*self).into();
        format!("SubmissionId(id={}, timestamp={})", self.id, submission_id.timestamp())
    }
}

impl From<SubmissionId> for submission::SubmissionId {
    fn from(val: SubmissionId) -> Self {
        submission::SubmissionId::from(val.id)
    }
}

impl From<submission::SubmissionId> for SubmissionId {
    fn from(val: submission::SubmissionId) -> Self {
        SubmissionId { id: val.into() }
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VecAsPyBytes(Vec<u8>);

impl IntoPy<PyObject> for VecAsPyBytes {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.as_slice().into_py(py)
    }
}

impl From<Vec<u8>> for VecAsPyBytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
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

// #[pyclass]
// pub struct PyChunksIter {
//     iter: Box<dyn Iterator<Item = PyResult<Vec<u8>>> + Send>,
// }

// impl PyChunksIter {
//     pub (crate) fn new_from_rust(iter: impl Iterator<Item = PyResult<Vec<u8>>> + Send + 'static) -> Self {
//         Self{iter: Box::new(iter)}
//     }
// }

// #[pymethods]
// impl PyChunksIter {
//     fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
//         slf
//     }

//     fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyResult<VecAsPyBytes>> {
//         slf.iter.next().map(|result| result.map(VecAsPyBytes))
//     }
// }
// #[pyclass]
// pub struct PyChunksStream {
//     iter: Box<dyn Iterator<Item = PyResult<Vec<u8>>> + Send>,
// }

// impl PyChunksStream {
//     pub (crate) fn new_from_rust<S>(stream: S, runtime: Arc<tokio::runtime::Runtime>) -> PyResult<Self>
//     where
//     S: Stream<Item = PyResult<Vec<u8>>> + TryStream<Ok = Vec<u8>, Error = PyErr> + Send + 'static
//     {
//         // It's rather annoying that we need _two_ boxes here,
//         // one for the box-pin and one to wrap the iterator.
//         // TODO: Investigate if there is a better way.
//         // One thing that comes to mind is to replace `from_fn`
//         // (which gives rise to a voldemort type and that is problematic as we cannot use generics with PyO3).
//         // with a dedicated home-written iterator struct.
//         let mut stream = Box::pin(stream);
//         let iter = std::iter::from_fn(move || {
//             runtime.block_on(stream.next())
//         });
//         Ok(Self{iter: Box::new(iter)})
//     }
// }

// #[pymethods]
// impl PyChunksStream {
//     fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
//         slf
//     }

//     fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyResult<VecAsPyBytes>> {
//         slf.iter.next().map(|result| result.map(VecAsPyBytes))
//     }
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

use std::future::IntoFuture;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::{stream, StreamExt, TryStreamExt};
use opsqueue::common::submission::Metadata;
use opsqueue::object_store::{ChunkType, ObjectStoreClient};
use pyo3::{create_exception, exceptions::PyException, prelude::*};

use opsqueue::common::{chunk, submission};
use opsqueue::consumer::client::OuterClient as ActualClient;
use opsqueue::consumer::strategy;

create_exception!(opsqueue_consumer_internal, ConsumerClientError, PyException);

// In development, check 10 times per second so we respond early to Ctrl+C
// But in production, only once per second so we don't fight as much over the GIL
#[cfg(debug_assertions)]
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(100);
#[cfg(not(debug_assertions))]
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_secs(1);

#[pyclass]
#[derive(Debug)]
struct Client {
    client: ActualClient,
    object_store_client: ObjectStoreClient,
    runtime: Arc<tokio::runtime::Runtime>,
}

fn maybe_wrap_error(e: anyhow::Error) -> PyErr {
    match e.downcast::<PyErr>() {
        Ok(py_err) => py_err,
        Err(other) => ConsumerClientError::new_err(other.to_string())
    }
}

#[pymethods]
impl Client {
    #[new]
    pub fn new(address: &str, object_store_url: &str) -> PyResult<Self> {
        let runtime = start_runtime();
        let client = ActualClient::new(address);
        let object_store_client = ObjectStoreClient::new(object_store_url).map_err(maybe_wrap_error)?;
        log::info!("Opsqueue consumer client initialized");

        Ok(Client {client, object_store_client, runtime })
    }

    pub fn reserve_chunks(&self, py: Python<'_>, max: usize, strategy: Strategy) -> PyResult<Vec<Chunk>> {
        py.allow_threads(|| { self.reserve_chunks_gilless(max, strategy)})
    }

    #[pyo3(signature = (submission_id, submission_prefix, chunk_index, output_content))]
    pub fn complete_chunk(
        &self,
        py: Python<'_>,
        submission_id: SubmissionId,
        submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        output_content: Vec<u8>,
    ) -> PyResult<()> {
        py.allow_threads(|| { self.complete_chunk_gilless(submission_id, submission_prefix, chunk_index, output_content)})
    }

    #[pyo3(signature = (submission_id, submission_prefix, chunk_index, failure))]
    pub fn fail_chunk(
        &self,
        py: Python<'_>,
        submission_id: SubmissionId,
        submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        failure: String,
    ) -> PyResult<()> {
        py.allow_threads(|| { self.fail_chunk_gilless(submission_id, submission_prefix, chunk_index, failure)})
    }

    pub fn run_per_chunk(&self, strategy: Strategy, fun: &Bound<'_, PyAny>) -> PyErr {
        if !fun.is_callable() {
            return pyo3::exceptions::PyTypeError::new_err("Expected `fun` parameter to be __call__-able");
        }
        // NOTE: We take care here to unlock the GIL for most of the loop,
        // and only re-lock it for the duration of each call to `fun`.
        let unbound_fun = fun.as_unbound();
        fun.py().allow_threads(|| {
            let mut done_count: usize = 0;
            loop {
                let chunk_outcome: PyResult<()> = (|| {
                    let chunks = self.reserve_chunks_gilless(1, strategy.clone())?;
                    log::debug!("Reserved {} chunks", chunks.len());
                    for chunk in chunks {
                        let submission_id = chunk.submission_id;
                        let submission_prefix = chunk.submission_prefix.clone();
                        let chunk_index = chunk.chunk_index;
                        log::debug!("Running fun for chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}", submission_id, chunk_index, &submission_prefix);
                        let res = Python::with_gil(|py| {
                            let res = unbound_fun.bind(py).call1((chunk,))?;
                            res.extract()
                        });
                        match res {
                            Ok(res) => {
                                log::debug!("Completing chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}", submission_id, chunk_index, &submission_prefix);
                                self.complete_chunk_gilless(submission_id, submission_prefix.clone(), chunk_index, res)?;
                                log::debug!("Completed chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}", submission_id, chunk_index, &submission_prefix);
                            },
                            Err(failure) => {
                                log::warn!("Failing chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}, reason: {failure:?}", submission_id, chunk_index, &submission_prefix);
                                self.fail_chunk_gilless(submission_id, submission_prefix.clone(), chunk_index, format!("{failure:?}"))?;
                                log::warn!("Failed chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}", submission_id, chunk_index, &submission_prefix);
                                // On exceptions that are not PyExceptions (but PyBaseExceptions), like KeyboardInterrupt etc, return.
                                // otherwise, continue with next chunk
                                if !Python::with_gil(|py| {failure.is_instance_of::<PyException>(py)}) {
                                    return Err(failure)
                                }
                            }
                        }

                        done_count = done_count.saturating_add(1);
                        if done_count % 50 == 0 {
                            log::info!("Processed {} chunks", done_count);
                        }
                    }
                    Ok(())
                })();

                // NOTE: We currently only quit on KeyboardInterrupt.
                // Any other error (like e.g. connection errors)
                // results in looping, which will re-establish the client connection.
                match chunk_outcome {
                    Ok(()) => {}
                    // In essence we 'catch `Exception` (but _not_ `BaseException` here)
                    Err(e) if Python::with_gil(|py| {e.is_instance_of::<PyException>(py)}) => {
                        log::warn!("Opsqueue consumer encountered an error, but will continue: {}", e);
                    }
                    Err(e) => {
                        log::info!("Opsqueue consumer closing because of exception: {e:?}");
                        return e
                    }
                }
            }
        })
    }

}

// What follows are internal helper functions
// that are not available directly from Python
impl Client {
    fn block_unless_interrupted<T, E>(&self, future: impl IntoFuture<Output = Result<T, E>>) -> Result<T, E>
    where
    E: From<PyErr>,
    {
        self.runtime.block_on(run_unless_interrupted(future))

    }

    fn sleep_unless_interrupted<E>(&self, duration: Duration) -> Result<(), E>
    where
        E: From<PyErr>
    {
        self.block_unless_interrupted(async {
            tokio::time::sleep(duration).await;
            Ok(())
        })
    }


    fn reserve_chunks_gilless(&self, max: usize, strategy: Strategy) -> PyResult<Vec<Chunk>> {
        // TODO: Currently we do short-polling here if there are no chunks available.
        // This is quite suboptimal; long-polling would be much nicer.
        const POLL_INTERVAL: Duration = Duration::from_millis(500);
        let strategy: strategy::Strategy = strategy.into();
        loop {
            let res: Vec<Chunk> =
                self.block_unless_interrupted(self.reserve_and_retrieve_chunks(max, strategy.clone()))
                .map_err(maybe_wrap_error)?;
            if !res.is_empty() {
                return Ok(res);
            }
            self.sleep_unless_interrupted::<PyErr>(POLL_INTERVAL)?;
        }
    }

    fn complete_chunk_gilless(
        &self,
        submission_id: SubmissionId,
        submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        output_content: Vec<u8>,
    ) -> PyResult<()> {
        let chunk_id = (submission_id.into(), chunk_index.into());
        self.block_unless_interrupted(async move {
            match submission_prefix {
                None =>
                    self.client.complete_chunk(chunk_id, Some(output_content)).await,
                Some(prefix) => {
                    self.object_store_client.store_chunk(&prefix, chunk_id.1, ChunkType::Output, output_content).await?;
                    self.client.complete_chunk(chunk_id, None).await
                }
            }
    })
            .map_err(maybe_wrap_error)
    }

    pub fn fail_chunk_gilless(
        &self,
        submission_id: SubmissionId,
        _submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        failure: String,
    ) -> PyResult<()> {
        let chunk_id = (submission_id.into(), chunk_index.into());
        self.block_unless_interrupted(self.client.fail_chunk(chunk_id, failure))
            .map_err(maybe_wrap_error)
    }

    async fn reserve_and_retrieve_chunks(&self, max: usize, strategy: opsqueue::consumer::strategy::Strategy) -> anyhow::Result<Vec<Chunk>>{
        let chunks = self.client.reserve_chunks(max, strategy).await?;
        let elems =
            stream::iter(chunks)
            .then(|(c, s)| Chunk::from_internal(c, s, &self.object_store_client))
            .try_collect().await?;
        Ok(elems)
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
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
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

#[pyclass(frozen, get_all, eq, ord, hash)]
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChunkIndex {
    pub id: i64,
}

#[pymethods]
impl ChunkIndex {
    #[new]
    fn new(id: i64) -> Self {
        Self { id }
    }

    fn __repr__(&self) -> String {
        format!("ChunkIndex(id={})", self.id)
    }
}

impl From<ChunkIndex> for chunk::ChunkIndex {
    fn from(val: ChunkIndex) -> Self {
        chunk::ChunkIndex::from(val.id)
    }
}

impl From<chunk::ChunkIndex> for ChunkIndex {
    fn from(val: chunk::ChunkIndex) -> Self {
        ChunkIndex { id: val.into() }
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

impl From<Strategy> for strategy::Strategy {
    fn from(val: Strategy) -> Self {
        match val {
            Strategy::Oldest => strategy::Strategy::Oldest,
            Strategy::Newest => strategy::Strategy::Newest,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentAsBytes(Vec<u8>);


impl IntoPy<PyObject> for ContentAsBytes {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let byteslice: &[u8] = self.0.as_ref();
        byteslice.into_py(py)
    }
}

/// Wrapper for the internal Opsqueue Chunk datatype
/// Note that it also includes some fields originating from the Submission
#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub submission_id: SubmissionId,
    pub chunk_index: ChunkIndex,
    pub input_content: ContentAsBytes,
    pub retries: i64,
    pub submission_prefix: Option<String>,
    pub submission_metadata: Option<Metadata>,
}

impl Chunk {
    pub async fn from_internal(c: chunk::Chunk, s: submission::Submission, object_store_client: &ObjectStoreClient) -> anyhow::Result<Self> {
        let (content, prefix) = match c.input_content {
            Some(bytes) => (bytes, None),
            None => {
                let prefix = s.prefix.unwrap();
                log::debug!("Fetching chunk content from object store: submission_id={}, prefix={}, chunk_index={}", c.submission_id, prefix, c.chunk_index);
                let res = object_store_client.retrieve_chunk(&prefix, c.chunk_index, ChunkType::Input).await?.to_vec();
                log::debug!("Fetched chunk content: {res:?}");
                (res, Some(prefix))
            }
        };
        Ok(Chunk {
            submission_id: c.submission_id.into(),
            chunk_index: c.chunk_index.into(),
            input_content: ContentAsBytes(content),
            retries: c.retries,
            submission_prefix: prefix,
            submission_metadata: s.metadata,
        })
    }

}

// impl From<chunk::Chunk> for Chunk {
//     fn from(value: chunk::Chunk) -> Self {
//         Self {
//             submission_id: value.submission_id.into(),
//             chunk_index: value.chunk_index.into(),
//             input_content: ContentAsBytes(value.input_content),
//             retries: value.retries,
//         }
//     }
// }

// impl Into<chunk::Chunk> for Chunk {
//     fn into(self) -> chunk::Chunk {
//         chunk::Chunk {
//             submission_id: self.submission_id.into(),
//             chunk_index: self.chunk_index.into(),
//             input_content: self.input_content.0,
//             retries: self.retries,
//         }
//     }
// }

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
        format!("{:?}", self)
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
    pub id: i64,
    pub chunks_total: i64,
    pub metadata: Option<submission::Metadata>,
    pub completed_at: DateTime<Utc>,
}

#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionFailed {
    pub id: i64,
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
        .expect("Failed to create Tokio runtime in opsqueue Consumer client");
    Arc::new(runtime)
}

/// A Python module implemented in Rust.
#[pymodule]
fn opsqueue_consumer_internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // We want Rust logs created by code called from this module
    // to be forwarded to Python's logging system
    pyo3_log::init();

    // Classes
    m.add_class::<Client>()?;
    m.add_class::<SubmissionId>()?;
    m.add_class::<ChunkIndex>()?;
    m.add_class::<Strategy>()?;
    m.add_class::<Chunk>()?;

    // Exception classes
    m.add(
        "ConsumerClientError",
        m.py().get_type_bound::<ConsumerClientError>(),
    )?;

    // Top-level functions
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}

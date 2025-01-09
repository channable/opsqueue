use std::future::IntoFuture;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use opsqueue::common::errors::TryFromIntError;
use opsqueue::common::submission::Metadata;
use opsqueue::object_store::{ChunkRetrievalError, ChunkType, ObjectStoreClient};
use opsqueue::tracing::CarrierMap;
use pyo3::prelude::*;

use opsqueue::common::{chunk, submission};
use opsqueue::consumer::strategy;
use ux_serde::u63;

use crate::errors::{CError, CPyResult, FatalPythonException};

// In development, check 10 times per second so we respond early to Ctrl+C
// But in production, only once per second so we don't fight as much over the GIL
#[cfg(debug_assertions)]
pub const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(100);
#[cfg(not(debug_assertions))]
pub const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_secs(1);

#[pyclass(frozen, get_all, eq, ord, hash)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct SubmissionId {
    pub id: u64,
}

#[pymethods]
impl SubmissionId {
    #[new]
    fn new(id: u64) -> CPyResult<Self, TryFromIntError> {
        let _is_inner_valid =
            opsqueue::common::submission::SubmissionId::try_from(id).map_err(CError)?;
        Ok(SubmissionId { id })
    }

    fn __repr__(&self) -> String {
        format!("SubmissionId(id={})", self.id)
    }
}

impl From<SubmissionId> for submission::SubmissionId {
    fn from(val: SubmissionId) -> Self {
        // NOTE: Previously constructed either through
        // `new` or an already-valid SubmissionId
        // so we can safely convert it back
        submission::SubmissionId::from(u63::new(val.id))
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
    pub id: u64,
}

#[pymethods]
impl ChunkIndex {
    #[new]
    fn new(id: u64) -> CPyResult<Self, TryFromIntError> {
        let _is_inner_valid = opsqueue::common::chunk::ChunkIndex::new(id).map_err(CError)?;
        Ok(ChunkIndex { id })
    }

    fn __repr__(&self) -> String {
        format!("ChunkIndex(id={})", self.id)
    }
}

impl From<ChunkIndex> for chunk::ChunkIndex {
    fn from(val: ChunkIndex) -> Self {
        // NOTE: Previously constructed either through
        // `new` or an already-valid SubmissionId
        // so we can safely convert it back
        chunk::ChunkIndex::from(u63::new(val.id))
    }
}

impl From<chunk::ChunkIndex> for ChunkIndex {
    fn from(val: chunk::ChunkIndex) -> Self {
        ChunkIndex::from(u63::from(val))
    }
}

impl From<u63> for ChunkIndex {
    fn from(value: u63) -> Self {
        ChunkIndex { id: value.into() }
    }
}

#[pyclass(frozen, eq)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreferDistinct {
    meta_key: String,
    underlying: Box<Strategy>,
}
#[pymethods]
impl PreferDistinct {
    #[new]
    fn new(meta_key: String, underlying: Strategy) -> Self {
        Self {
            meta_key,
            underlying: Box::new(underlying),
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pyclass(frozen, eq)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Strategy {
    #[pyo3(constructor=())]
    Oldest(),
    #[pyo3(constructor=())]
    Newest(),
    #[pyo3(constructor=())]
    Random(),
    #[pyo3(constructor=(_0))]
    PreferDistinct(PreferDistinct),
}

impl From<strategy::Strategy> for Strategy {
    fn from(value: strategy::Strategy) -> Self {
        match value {
            strategy::Strategy::Oldest => Strategy::Oldest(),
            strategy::Strategy::Newest => Strategy::Newest(),
            strategy::Strategy::Random => Strategy::Random(),
            strategy::Strategy::PreferDistinct {
                meta_key,
                underlying,
            } => {
                let underlying = Strategy::from(*underlying);
                Strategy::PreferDistinct(PreferDistinct {
                    meta_key,
                    underlying: Box::new(underlying),
                })
            }
        }
    }
}

impl From<Strategy> for strategy::Strategy {
    fn from(val: Strategy) -> Self {
        match val {
            Strategy::Oldest() => strategy::Strategy::Oldest,
            Strategy::Newest() => strategy::Strategy::Newest,
            Strategy::Random() => strategy::Strategy::Random,
            Strategy::PreferDistinct(PreferDistinct {
                meta_key,
                underlying,
            }) => {
                let underlying = strategy::Strategy::from(*underlying);
                strategy::Strategy::PreferDistinct {
                    meta_key,
                    underlying: Box::new(underlying),
                }
            }
        }
    }
}

/// Wrapper for the internal Opsqueue Chunk datatype
/// Note that it also includes some fields originating from the Submission
#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub submission_id: SubmissionId,
    pub chunk_index: ChunkIndex,
    pub input_content: VecAsPyBytes,
    pub retries: i64,
    pub submission_prefix: Option<String>,
    pub submission_metadata: Option<Metadata>,
    pub submission_otel_trace_carrier: CarrierMap,
}

impl Chunk {
    pub async fn from_internal(
        c: chunk::Chunk,
        s: submission::Submission,
        object_store_client: &ObjectStoreClient,
    ) -> Result<Self, ChunkRetrievalError> {
        let (content, prefix) = match c.input_content {
            Some(bytes) => (bytes, None),
            None => {
                let prefix = s.prefix.unwrap();
                log::debug!("Fetching chunk content from object store: submission_id={}, prefix={}, chunk_index={}", c.submission_id, prefix, c.chunk_index);
                let res = object_store_client
                    .retrieve_chunk(&prefix, c.chunk_index, ChunkType::Input)
                    .await?
                    .to_vec();
                log::debug!("Fetched chunk content: {res:?}");
                (res, Some(prefix))
            }
        };
        Ok(Chunk {
            submission_id: c.submission_id.into(),
            chunk_index: c.chunk_index.into(),
            input_content: VecAsPyBytes(content),
            retries: c.retries,
            submission_prefix: prefix,
            submission_metadata: s.metadata,
            submission_otel_trace_carrier: opsqueue::tracing::json_to_carrier(
                &s.otel_trace_carrier,
            ),
        })
    }
}

#[pymethods]
impl Chunk {
    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

/// Wrapper for the internal Opsqueue Chunk datatype
/// Note that it also includes some fields originating from the Submission
#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkFailed {
    pub submission_id: SubmissionId,
    pub chunk_index: ChunkIndex,
    pub failure: String,
    pub failed_at: DateTime<Utc>,
}

impl ChunkFailed {
    pub fn from_internal(c: chunk::ChunkFailed, _s: &submission::SubmissionFailed) -> Self {
        ChunkFailed {
            submission_id: c.submission_id.into(),
            chunk_index: c.chunk_index.into(),
            failure: c.failure,
            failed_at: c.failed_at,
        }
    }
}

#[pymethods]
impl ChunkFailed {
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

impl From<opsqueue::common::submission::SubmissionCompleted> for SubmissionCompleted {
    fn from(value: opsqueue::common::submission::SubmissionCompleted) -> Self {
        Self {
            id: value.id.into(),
            completed_at: value.completed_at,
            chunks_total: value.chunks_total.into(),
            metadata: value.metadata,
        }
    }
}

impl From<opsqueue::common::submission::SubmissionFailed> for SubmissionFailed {
    fn from(value: opsqueue::common::submission::SubmissionFailed) -> Self {
        Self {
            id: value.id.into(),
            failed_at: value.failed_at,
            chunks_total: value.chunks_total.into(),
            metadata: value.metadata,
            failed_chunk_id: value.failed_chunk_id.into(),
        }
    }
}

#[pyclass(frozen)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionStatus {
    InProgress {
        submission: Submission,
    },
    Completed {
        submission: SubmissionCompleted,
    },
    Failed {
        submission: SubmissionFailed,
        chunk: ChunkFailed,
    },
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
            Failed(s, c) => {
                let chunk = ChunkFailed::from_internal(c, &s);
                let submission = s.into();
                SubmissionStatus::Failed { submission, chunk }
            }
        }
    }
}

#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    pub id: SubmissionId,
    pub chunks_total: u64,
    pub chunks_done: u64,
    pub metadata: Option<submission::Metadata>,
}

impl From<opsqueue::common::submission::Submission> for Submission {
    fn from(value: opsqueue::common::submission::Submission) -> Self {
        Self {
            id: value.id.into(),
            chunks_total: value.chunks_total.into(),
            chunks_done: value.chunks_done.into(),
            metadata: value.metadata,
        }
    }
}

#[pymethods]
impl Submission {
    fn __repr__(&self) -> String {
        format!(
            "Submission(id={0}, chunks_total={1}, chunks_done={2}, metadata={3:?})",
            self.id.__repr__(),
            self.chunks_total,
            self.chunks_done,
            self.metadata
        )
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
        format!(
            "SubmissionCompleted(id={0}, chunks_total={1}, completed_at={2}, metadata={3:?})",
            self.id.__repr__(),
            self.chunks_total,
            self.completed_at,
            self.metadata
        )
    }
}

#[pymethods]
impl SubmissionFailed {
    fn __repr__(&self) -> String {
        format!("SubmissionFailed(id={0}, chunks_total={1}, failed_at={2}, failed_chunk_id={3}, metadata={4:?})",
        self.id.__repr__(), self.chunks_total, self.failed_at, self.failed_chunk_id, self.metadata)
    }
}

#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionCompleted {
    pub id: SubmissionId,
    pub chunks_total: u64,
    pub metadata: Option<submission::Metadata>,
    pub completed_at: DateTime<Utc>,
}

#[pyclass(frozen, get_all)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionFailed {
    pub id: SubmissionId,
    pub chunks_total: u64,
    pub metadata: Option<submission::Metadata>,
    pub failed_at: DateTime<Utc>,
    pub failed_chunk_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VecAsPyBytes(pub Vec<u8>);

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

pub async fn run_unless_interrupted<T, E>(
    future: impl IntoFuture<Output = Result<T, E>>,
) -> Result<T, E>
where
    E: From<FatalPythonException>,
{
    tokio::select! {
        res = future => res,
        py_err = check_signals_in_background() => Err(py_err)?,
    }
}

pub async fn check_signals_in_background() -> FatalPythonException {
    loop {
        tokio::time::sleep(SIGNAL_CHECK_INTERVAL).await;
        if let Err(err) = Python::with_gil(|py| py.check_signals()) {
            return err.into();
        }
    }
}

pub fn start_runtime() -> Arc<tokio::runtime::Runtime> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime in opsqueue client");
    Arc::new(runtime)
}

/// Formats a Python exception
/// in similar fashion as the traceback.format_exc()
/// would do it.
///
/// Internally acquires the GIL!
///
/// c.f. https://pyo3.rs/main/doc/pyo3/types/trait.pytracebackmethods
pub fn format_pyerr(err: &PyErr) -> String {
    Python::with_gil(|py| {
        let msg: Option<String> = (|| {
            let traceback = err.traceback_bound(py)?;
            let traceback_str = traceback
                .format()
                .expect("Tracebacks are always formattable");
            let str = format!("{}{}", traceback_str, err);
            Some(str)
        })();
        msg.unwrap_or_else(|| format!("{}", err))
    })
}

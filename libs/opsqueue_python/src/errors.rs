use std::error::Error;

use opsqueue::common::errors::{
    ChunkNotFound, E, IncorrectUsage, SubmissionNotFound,
    UnexpectedOpsqueueConsumerServerResponse,
};
use opsqueue::common::NonZeroIsZero;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::PyErr;
use pyo3::{create_exception, IntoPy, PyObject};

use crate::common::{ChunkIndex, SubmissionId};

create_exception!(opsqueue_internal, IncorrectUsageError, PyTypeError);
create_exception!(
    opsqueue_internal,
    SubmissionNotFoundError,
    IncorrectUsageError
);
create_exception!(opsqueue_internal, ChunkNotFoundError, IncorrectUsageError);
create_exception!(
    opsqueue_internal,
    InvalidChunkIndexError,
    IncorrectUsageError
);
create_exception!(
    opsqueue_internal,
    ChunkCountIsZeroError,
    IncorrectUsageError
);
create_exception!(
    opsqueue_internal,
    NewObjectStoreClientError,
    IncorrectUsageError
);
create_exception!(
    opsqueue_internal,
    SubmissionNotCompletedYetError,
    IncorrectUsageError
);

create_exception!(opsqueue_internal, OpsqueueInternalError, PyException);
create_exception!(
    opsqueue_internal,
    UnexpectedOpsqueueConsumerServerResponseError,
    OpsqueueInternalError
);
create_exception!(
    opsqueue_internal,
    ChunkRetrievalError,
    OpsqueueInternalError
);
create_exception!(opsqueue_internal, ChunkStorageError, OpsqueueInternalError);
create_exception!(opsqueue_internal, ChunksStorageError, OpsqueueInternalError);
create_exception!(
    opsqueue_internal,
    InternalConsumerClientError,
    OpsqueueInternalError
);
create_exception!(
    opsqueue_internal,
    InternalProducerClientError,
    OpsqueueInternalError
);

/// A newtype so we can write From/Into implementations turning various error types
/// into PyErr, including those defined in other crates.
///
/// This follows the 'newtype wrapper' approach from
/// https://pyo3.rs/v0.22.5/function/error-handling#foreign-rust-error-types
///
/// The 'C' stands for 'Convertible'.
pub struct CError<T>(pub T);
impl<T> From<T> for CError<T> {
    fn from(value: T) -> Self {
        CError(value)
    }
}

/// Result type alias to help with the automatic conversion of error types
/// into PyErr.
///
/// This follows the 'newtype wrapper' approach from
/// https://pyo3.rs/v0.22.5/function/error-handling#foreign-rust-error-types
///
/// The 'C' stands for 'Convertible'.
pub type CPyResult<T, E> = Result<T, CError<E>>;

/// Indicates a 'fatal' PyErr: Any Python exception which is _not_ a subclass of `PyException`.
///
/// These are known as 'fatal' exceptions in Python.
/// c.f. https://docs.python.org/3/tutorial/errors.html#tut-userexceptions
///
/// We don't consume/wrap these errors but propagate them,
/// allowing things like KeyboardInterrupt, SystemExit or MemoryError,
/// to trigger cleanup-and-exit.
#[derive(thiserror::Error, Debug)]
#[error("Fatal Python exception: {0}")]
pub struct FatalPythonException(#[from] pub PyErr);

impl From<CError<FatalPythonException>> for PyErr {
    fn from(value: CError<FatalPythonException>) -> Self {
        value.0 .0
    }
}

impl From<FatalPythonException> for PyErr {
    fn from(value: FatalPythonException) -> Self {
        value.0
    }
}

impl From<CError<opsqueue::common::chunk::InvalidChunkIndexError>> for PyErr {
    fn from(value: CError<opsqueue::common::chunk::InvalidChunkIndexError>) -> Self {
        InvalidChunkIndexError::new_err((value.0.to_string(), value.0 .0)).into()
    }
}

impl From<CError<opsqueue::consumer::client::InternalConsumerClientError>> for PyErr {
    fn from(value: CError<opsqueue::consumer::client::InternalConsumerClientError>) -> Self {
        InternalConsumerClientError::new_err(value.0.to_string()).into()
    }
}

impl From<CError<opsqueue::producer::client::InternalProducerClientError>> for PyErr {
    fn from(value: CError<opsqueue::producer::client::InternalProducerClientError>) -> Self {
        InternalProducerClientError::new_err(value.0.to_string()).into()
    }
}

impl From<CError<opsqueue::object_store::ChunkRetrievalError>> for PyErr {
    fn from(value: CError<opsqueue::object_store::ChunkRetrievalError>) -> Self {
        ChunkRetrievalError::new_err(value.0.to_string()).into()
    }
}

impl From<CError<opsqueue::object_store::ChunksStorageError>> for PyErr {
    fn from(value: CError<opsqueue::object_store::ChunksStorageError>) -> Self {
        ChunksStorageError::new_err(value.0.to_string()).into()
    }
}

impl From<CError<opsqueue::object_store::ChunkStorageError>> for PyErr {
    fn from(value: CError<opsqueue::object_store::ChunkStorageError>) -> Self {
        ChunkStorageError::new_err(value.0.to_string()).into()
    }
}

impl From<CError<NonZeroIsZero<opsqueue::common::chunk::ChunkIndex>>> for PyErr {
    fn from(value: CError<NonZeroIsZero<opsqueue::common::chunk::ChunkIndex>>) -> Self {
        ChunkCountIsZeroError::new_err(value.0.to_string()).into()
    }
}

impl<T: Error> From<CError<IncorrectUsage<T>>> for PyErr {
    fn from(value: CError<IncorrectUsage<T>>) -> Self {
        IncorrectUsageError::new_err(value.0.to_string()).into()
    }
}

impl From<CError<SubmissionNotFound>> for PyErr {
    fn from(value: CError<SubmissionNotFound>) -> Self {
        let submission_id = value.0 .0;
        SubmissionNotFoundError::new_err((value.0.to_string(), SubmissionId::from(submission_id)))
            .into()
    }
}

impl From<CError<crate::producer::SubmissionNotCompletedYetError>> for PyErr {
    fn from(value: CError<crate::producer::SubmissionNotCompletedYetError>) -> Self {
        let submission_id = value.0 .0;
        SubmissionNotCompletedYetError::new_err((
            value.0.to_string(),
            SubmissionId::from(submission_id),
        ))
        .into()
    }
}

impl From<CError<ChunkNotFound>> for PyErr {
    fn from(value: CError<ChunkNotFound>) -> Self {
        let (submission_id, chunk_index) = value.0 .0;
        ChunkNotFoundError::new_err((
            value.0.to_string(),
            (
                SubmissionId::from(submission_id),
                ChunkIndex::from(chunk_index),
            ),
        ))
        .into()
    }
}

impl From<CError<opsqueue::object_store::NewObjectStoreClientError>> for PyErr {
    fn from(value: CError<opsqueue::object_store::NewObjectStoreClientError>) -> Self {
        NewObjectStoreClientError::new_err(value.0.to_string()).into()
    }
}

// TODO: Only temporary. We want to get rid of all usage of anyhow
// in the boundary to PyO3
impl From<CError<anyhow::Error>> for PyErr {
    fn from(value: CError<anyhow::Error>) -> Self {
        PyException::new_err(value.0.to_string()).into()
    }
}

impl From<CError<UnexpectedOpsqueueConsumerServerResponse>> for PyErr {
    fn from(value: CError<UnexpectedOpsqueueConsumerServerResponse>) -> Self {
        UnexpectedOpsqueueConsumerServerResponseError::new_err(value.0.to_string()).into()
    }
}

impl<T> From<PyErr> for CError<E<FatalPythonException, T>> {
    fn from(value: PyErr) -> Self {
        CError(E::L(FatalPythonException(value)))
    }
}

impl<T> From<FatalPythonException> for CError<E<FatalPythonException, T>> {
    fn from(value: FatalPythonException) -> Self {
        CError(E::L(value))
    }
}

impl<L, R> From<CError<E<L, R>>> for PyErr
where
    PyErr: From<CError<L>> + From<CError<R>>, // CError<L>: Into<PyErr>,
                                              // CError<R>: Into<PyErr>,
{
    fn from(value: CError<E<L, R>>) -> Self {
        match value.0 {
            E::L(e) => CError(e).into(),
            E::R(e) => CError(e).into(),
        }
    }
}

impl From<CError<PyErr>> for PyErr {
    fn from(value: CError<PyErr>) -> Self {
        value.0
    }
}

impl<T> IntoPy<PyObject> for CError<T>
where
    CError<T>: Into<PyErr>,
{
    fn into_py(self, py: pyo3::Python<'_>) -> PyObject {
        CError(self.0).into().into_py(py)
    }
}

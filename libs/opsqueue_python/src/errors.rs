/// NOTE: We defne the potentially raisable errors/exceptions in Python
/// so we have nice IDE support for docs-on-hover and for 'go to definition'.
use std::error::Error;

use opsqueue::common::chunk::ChunkId;
use opsqueue::common::errors::{
    ChunkNotFound, IncorrectUsage, SubmissionNotFound, UnexpectedOpsqueueConsumerServerResponse, E,
};
use pyo3::exceptions::PyBaseException;
use pyo3::{import_exception, Bound, PyErr, Python};

use crate::common::{ChunkIndex, SubmissionId};

// Expected errors:
import_exception!(opsqueue.exceptions, SubmissionFailedError);

// Incorrect usage errors:
import_exception!(opsqueue.exceptions, IncorrectUsageError);
import_exception!(opsqueue.exceptions, TryFromIntError);
import_exception!(opsqueue.exceptions, ChunkNotFoundError);
import_exception!(opsqueue.exceptions, SubmissionNotFoundError);
import_exception!(opsqueue.exceptions, NewObjectStoreClientError);
import_exception!(opsqueue.exceptions, SubmissionNotCompletedYetError);

// Internal errors:
import_exception!(opsqueue.exceptions, OpsqueueInternalError);
import_exception!(
    opsqueue.exceptions,
    UnexpectedOpsqueueConsumerServerResponseError
);
import_exception!(opsqueue.exceptions, ChunkRetrievalError);
import_exception!(opsqueue.exceptions, ChunksStorageError);
import_exception!(opsqueue.exceptions, ChunkStorageError);
import_exception!(opsqueue.exceptions, InternalConsumerClientError);
import_exception!(opsqueue.exceptions, InternalProducerClientError);

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

impl From<CError<opsqueue::common::errors::TryFromIntError>> for PyErr {
    fn from(value: CError<opsqueue::common::errors::TryFromIntError>) -> Self {
        TryFromIntError::new_err(value.0.to_string())
    }
}

impl From<CError<opsqueue::consumer::client::InternalConsumerClientError>> for PyErr {
    fn from(value: CError<opsqueue::consumer::client::InternalConsumerClientError>) -> Self {
        InternalConsumerClientError::new_err(value.0.to_string())
    }
}

impl From<CError<opsqueue::producer::client::InternalProducerClientError>> for PyErr {
    fn from(value: CError<opsqueue::producer::client::InternalProducerClientError>) -> Self {
        InternalProducerClientError::new_err(value.0.to_string())
    }
}

impl From<CError<opsqueue::object_store::ChunkRetrievalError>> for PyErr {
    fn from(value: CError<opsqueue::object_store::ChunkRetrievalError>) -> Self {
        ChunkRetrievalError::new_err(value.0.to_string())
    }
}

impl From<CError<opsqueue::object_store::ChunksStorageError>> for PyErr {
    fn from(value: CError<opsqueue::object_store::ChunksStorageError>) -> Self {
        ChunksStorageError::new_err(value.0.to_string())
    }
}

impl From<CError<opsqueue::object_store::ChunkStorageError>> for PyErr {
    fn from(value: CError<opsqueue::object_store::ChunkStorageError>) -> Self {
        ChunkStorageError::new_err(value.0.to_string())
    }
}

impl<T: Error> From<CError<IncorrectUsage<T>>> for PyErr {
    fn from(value: CError<IncorrectUsage<T>>) -> Self {
        IncorrectUsageError::new_err(value.0.to_string())
    }
}

impl From<CError<SubmissionNotFound>> for PyErr {
    fn from(value: CError<SubmissionNotFound>) -> Self {
        let submission_id = value.0 .0;
        SubmissionNotFoundError::new_err((value.0.to_string(), SubmissionId::from(submission_id)))
    }
}

pub struct SubmissionFailed(
    pub crate::common::SubmissionFailed,
    pub crate::common::ChunkFailed,
);

impl From<CError<SubmissionFailed>> for PyErr {
    fn from(value: CError<SubmissionFailed>) -> Self {
        let submission: crate::common::SubmissionFailed = value.0 .0;
        let chunk: crate::common::ChunkFailed = value.0 .1;
        SubmissionFailedError::new_err((submission, chunk))
    }
}

impl From<CError<crate::producer::SubmissionNotCompletedYetError>> for PyErr {
    fn from(value: CError<crate::producer::SubmissionNotCompletedYetError>) -> Self {
        let submission_id = value.0 .0;
        SubmissionNotCompletedYetError::new_err((value.0.to_string(), submission_id))
    }
}

impl From<CError<ChunkNotFound>> for PyErr {
    fn from(value: CError<ChunkNotFound>) -> Self {
        let ChunkId {
            submission_id,
            chunk_index,
        } = value.0 .0;
        ChunkNotFoundError::new_err((
            value.0.to_string(),
            (
                SubmissionId::from(submission_id),
                ChunkIndex::from(chunk_index),
            ),
        ))
    }
}

impl From<CError<opsqueue::object_store::NewObjectStoreClientError>> for PyErr {
    fn from(value: CError<opsqueue::object_store::NewObjectStoreClientError>) -> Self {
        NewObjectStoreClientError::new_err(value.0.to_string())
    }
}

impl From<CError<UnexpectedOpsqueueConsumerServerResponse>> for PyErr {
    fn from(value: CError<UnexpectedOpsqueueConsumerServerResponse>) -> Self {
        UnexpectedOpsqueueConsumerServerResponseError::new_err(value.0.to_string())
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

impl<'py, T> pyo3::IntoPyObject<'py> for CError<T>
where
    CError<T>: Into<PyErr>,
{
    type Target = PyBaseException;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(CError(self.0).into().value(py).clone())
    }
}

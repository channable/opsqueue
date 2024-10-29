use std::error::Error;

use either::Either;
use opsqueue::common::errors::{ChunkNotFound, DBErrorOr, IncorrectUsage, SubmissionNotFound};
use pyo3::{create_exception, IntoPy, PyObject};
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::PyErr;

create_exception!(opsqueue_internal, DatabaseError, PyException);
create_exception!(opsqueue_internal, IncorrectUsageError, PyTypeError);
create_exception!(opsqueue_internal, SubmissionNotFoundError, IncorrectUsageError);
create_exception!(opsqueue_internal, ChunkNotFoundError, IncorrectUsageError);

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

impl<T> From<CError<DBErrorOr<T>>> for PyErr
where
  CError<T>: Into<PyErr>,
{
    fn from(value: CError<DBErrorOr<T>>) -> Self {
        match value.0 {
            DBErrorOr::Database(e) => DatabaseError::new_err(e.to_string()).into(),
            DBErrorOr::Other(e) => CError(e).into(),
        }
    }
}

impl<T: Error> From<CError<IncorrectUsage<T>>> for PyErr {
    fn from(value: CError<IncorrectUsage<T>>) -> Self {
        IncorrectUsageError::new_err(value.0.to_string()).into()
    }
}

impl From<CError<SubmissionNotFound>> for PyErr {
    fn from(value: CError<SubmissionNotFound>) -> Self {
        SubmissionNotFoundError::new_err(value.0.to_string()).into()
    }
}

impl From<CError<ChunkNotFound>> for PyErr {
    fn from(value: CError<ChunkNotFound>) -> Self {
        ChunkNotFoundError::new_err(value.0.to_string()).into()
    }
}

impl<T> From<PyErr> for CError<Either<PyErr, T>> {
    fn from(value: PyErr) -> Self {
        CError(Either::Left(value))
    }
}

impl <L, R> From<CError<Either<L, R>>> for PyErr 
where
PyErr: From<CError<L>> + From<CError<R>>
// CError<L>: Into<PyErr>,
// CError<R>: Into<PyErr>,
{
    fn from(value: CError<Either<L, R>>) -> Self {
        match value.0 {
            Either::Left(e) => CError(e).into(),
            Either::Right(e) => CError(e).into(),
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

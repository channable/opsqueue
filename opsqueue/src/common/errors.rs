use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::consumer::common::SyncServerToClientResponse;

use super::{chunk::ChunkId, submission::SubmissionId};

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
#[error("Low-level database error: {0}")]
pub struct DatabaseError(#[from] pub serde_error::Error);

#[derive(Error, Debug)]
#[error("Unexpected opsqueue consumer server response. This indicates an error inside Opsqueue itself: {0:?}")]
pub struct UnexpectedOpsqueueConsumerServerResponse(pub SyncServerToClientResponse);

impl From<sqlx::Error> for DatabaseError {
    fn from(value: sqlx::Error) -> Self {
        DatabaseError(serde_error::Error::new(&value))
    }
}

#[derive(Error, Debug)]
#[error("Chunk not found for ID {0:?}")]
pub struct ChunkNotFound(pub ChunkId);

#[derive(Error, Debug)]
#[error("Submission not found for ID {0:?}")]
pub struct SubmissionNotFound(pub SubmissionId);

use either::Either;
impl<T> From<DatabaseError> for Either<DatabaseError, T> {
    fn from(e: DatabaseError) -> Self {
        Either::Left(e)
    }
}

/// This explicit named type is introduced because we _need_ a `From<sqlx::Error>` instance
/// to be able to use an error type inside a closure passed to `SqliteConnection.transaction()`.
/// 
/// For all intents and purposes, treat it as `Either<DatabaseError, T>`.
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum DBErrorOr<T> {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Other(T),
}

impl<T> DBErrorOr<T> {
    pub fn map_other<R>(self, f: impl FnOnce(T) -> R) -> DBErrorOr<R> {
        match self {
            DBErrorOr::Database(e) => DBErrorOr::Database(e),
            DBErrorOr::Other(t) => DBErrorOr::Other(f(t)),
        }
    }
}

impl<T> From<sqlx::Error> for DBErrorOr<T> {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(DatabaseError::from(value))
    }
}

impl<L, R> From<Either<L, R>> for DBErrorOr<Either<L, R>> {
    fn from(value: Either<L, R>) -> Self {
        Self::Other(value)
    }
}

#[derive(Error, Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[error("You are using Opsqueue incorrectly. Details: {0}")]
pub struct IncorrectUsage<E>(#[from] pub E);

#[derive(Error, Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[error("You passed a 0 as reservation maximum limit. Please provide a positive integer")]
pub struct LimitIsZero();

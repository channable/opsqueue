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

impl<T> From<DatabaseError> for Either<DatabaseError, T> {
    fn from(e: DatabaseError) -> Self {
        Either::Left(e)
    }
}

/// We roll our own version of `either::Either` so that we're not limited by the orphan rule.
///
/// We only use this particular Either type for error handling in the case we have a result returning two or more
/// potential errors.
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum Either<L, R> {
    #[error(transparent)]
    Left(L),
    #[error(transparent)]
    Right(R),
}

/// Builds a nested `Either` from two or more (error) types.
/// - `E![A, B]` is the same as `Either<A, B>`
/// - `E![A, B, C]` is the same as `Either<A, Either<B, C>>`
/// - etc.
#[macro_export]
macro_rules! E {
    ($tl: ty, $tr: ty) => ($crate::common::errors::Either<$tl, $tr>);
    ($h:ty, $($t:ty),+ $(,)?) => ($crate::common::errors::Either<$h, $crate::E!($($t),+)>);
}

/// Allows you to run the same expression on both halves of an Either,
/// without the types necessarily having to match.
///
/// For example, to run `Into::into` on both halves, we cannot just pass a single function
/// because that would restrict L and R to be the same type.
///
/// Instead, you can use
///
/// ```rust
/// map_both!(either, variant => variant.into())
/// ```
/// which will desugar to
/// ```rust
/// match either {
///   Either::Left(variant) => Either::Left(variant.into()),
///   Either::Right(variant) => Either::Right(variant.into()),
/// }
/// ```
#[macro_export]
macro_rules! map_both {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            $crate::common::errors::Either::Left($pattern) => Either::Left($result),
            $crate::common::errors::Either::Right($pattern) => Either::Right($result),
        }
    };
}

/// Similar to `map_both` but doesn't wrap the result back in the respective Left/Right variant.
#[macro_export]
macro_rules! fold_both {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            $crate::common::errors::Either::Left($pattern) => $result,
            $crate::common::errors::Either::Right($pattern) => $result,
        }
    };
}

impl<R> From<sqlx::Error> for Either<DatabaseError, R> {
    fn from(value: sqlx::Error) -> Self {
        Either::Left(DatabaseError::from(value))
    }
}

impl<L, R1, R2> From<Either<R1, R2>> for Either<L, Either<R1, R2>> {
    fn from(value: Either<R1, R2>) -> Self {
        Either::Right(value)
    }
}

#[derive(Error, Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[error("You are using Opsqueue incorrectly. Details: {0}")]
pub struct IncorrectUsage<E>(#[from] pub E);

#[derive(Error, Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[error("You passed a 0 as reservation maximum limit. Please provide a positive integer")]
pub struct LimitIsZero();

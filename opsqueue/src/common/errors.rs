//! Common error types
//!
//! You might notice in many places in Opsqueue that we use very fine-grained error types,
//! and combine them together using the `E` helper.
//!
//! This is a conscious choice: While it makes some function signatures more complex,
//! it allows us to be super precise in what kind of errors can and cannot occur
//! in certain API calls.
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::consumer::common::SyncServerToClientResponse;

use super::{chunk::ChunkId, submission::SubmissionId};

// #[derive(Error, Debug, Clone, Serialize, Deserialize)]
// #[error("Low-level database error: {0:?}")]
// pub struct DatabaseError(#[from] pub serde_error::Error);
#[cfg_attr(feature = "server-logic", derive(Error, Debug))]
#[cfg_attr(feature = "server-logic", error("Low-level database error: {0:?}"))]
#[cfg(feature = "server-logic")]
pub struct DatabaseError(#[from] pub sqlx::Error);

#[cfg(feature = "server-logic")]
impl<T> From<DatabaseError> for E<DatabaseError, T> {
    fn from(e: DatabaseError) -> Self {
        E::L(e)
    }
}

#[derive(Error, Debug)]
#[error("Chunk not found for ID {0:?}")]
pub struct ChunkNotFound(pub ChunkId);

#[derive(Error, Debug)]
#[error("Submission not found for ID {0:?}")]
pub struct SubmissionNotFound(pub SubmissionId);

#[derive(Error, Debug)]
#[error("Unexpected opsqueue consumer server response. This indicates an error inside Opsqueue itself: {0:?}")]
pub struct UnexpectedOpsqueueConsumerServerResponse(pub SyncServerToClientResponse);

/// We roll our own version of `either::E` so that we're not limited by the orphan rule.
///
/// We only use this particular E type for error handling in the case we have a result returning two or more
/// potential errors.
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum E<L, R> {
    /// A abbreviation for Left
    #[error(transparent)]
    L(L),
    /// An abbreviation for Right
    #[error(transparent)]
    R(R),
}

/// Builds a nested `E` from two or more (error) types.
/// - `E![A, B]` is the same as `E<A, B>`
/// - `E![A, B, C]` is the same as `E<A, E<B, C>>`
/// - etc.
#[macro_export]
macro_rules! E {
    ($tl: ty, $tr: ty) => ($crate::common::errors::E<$tl, $tr>);
    ($h:ty, $($t:ty),+ $(,)?) => ($crate::common::errors::E<$h, $crate::E!($($t),+)>);
}

/// Allows you to run the same expression on both halves of an E,
/// without the types necessarily having to match.
///
/// For example, to run `Into::into` on both halves, we cannot just pass a single function
/// because that would restrict L and R to be the same type.
///
/// Instead, you can use
///
/// ```ignore
/// map_both!(either, variant => variant.into())
/// ```
/// which will desugar to
/// ```ignore
/// match either {
///   E::L(variant) => E::L(variant.into()),
///   E::R(variant) => E::R(variant.into()),
/// }
/// ```
#[macro_export]
macro_rules! map_both {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            $crate::common::errors::E::L($pattern) => $crate::common::errors::E::L($result),
            $crate::common::errors::E::R($pattern) => $crate::common::errors::E::R($result),
        }
    };
}

/// Similar to `map_both` but doesn't wrap the result back in the respective Left/Right variant.
#[macro_export]
macro_rules! fold_both {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            $crate::common::errors::E::L($pattern) => $result,
            $crate::common::errors::E::R($pattern) => $result,
        }
    };
}

#[cfg(feature = "server-logic")]
impl<R> From<sqlx::Error> for E<DatabaseError, R> {
    fn from(value: sqlx::Error) -> Self {
        E::L(DatabaseError::from(value))
    }
}

impl<L, R1, R2> From<E<R1, R2>> for E<L, E<R1, R2>> {
    fn from(value: E<R1, R2>) -> Self {
        E::R(value)
    }
}

#[derive(Error, Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[error("You are using Opsqueue incorrectly. Details: {0}")]
pub struct IncorrectUsage<E>(#[from] pub E);

#[derive(Error, Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[error("You passed a 0 as reservation maximum limit. Please provide a positive integer")]
pub struct LimitIsZero();

/// Similar to the type in the stdlib, used with our custom ID int-wrapper types
/// (`SubmissionId`, `ChunkIndex`, etc.)
#[derive(thiserror::Error, Debug, Copy, Clone, PartialEq, Eq)]
#[error("out of range integral type conversion attempted")]
pub struct TryFromIntError(pub(crate) ());

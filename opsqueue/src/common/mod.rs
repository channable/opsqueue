//! Common datatypes and errors shared across all parts of Opsqueue
use rustc_hash::FxHashMap;
use std::num::NonZero;

pub mod chunk;
pub mod errors;
pub mod submission;

/// As values, we support the largest number value SQLite supports by itself,
/// which should be sufficient for most 'ID' fields, which is what this feature is intended for.
///
/// If you really need to use strings or UUIDs with a `PreferDistinct` strategy,
/// consider hashing them and using that hash as MetaStateVal.
pub type MetaStateVal = i64;
pub type StrategicMetadataMap = FxHashMap<String, MetaStateVal>;

/// Maximum number of submissions a lookup may return.
/// Guarantees: 0 < MaxSubmissions 1 < i64::MAX;
#[derive(Debug, Clone, Copy)]
pub struct MaxSubmissions(NonZero<u64>);

impl MaxSubmissions {
    pub fn new(value: NonZero<u64>) -> Result<Self, MaxSubmissionsTooLarge> {
        if u64::from(value) < i64::MAX as u64 {
            Ok(Self(value))
        } else {
            Err(MaxSubmissionsTooLarge(value))
        }
    }
}

impl From<MaxSubmissions> for u64 {
    fn from(max_submissions: MaxSubmissions) -> u64 {
        u64::from(max_submissions.0)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("max_submissions value {0} is too large; it must be at most i64::MAX - 1")]
pub struct MaxSubmissionsTooLarge(pub NonZero<u64>);

impl std::fmt::Display for MaxSubmissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseMaxSubmissionsError {
    #[error(transparent)]
    NotANumber(#[from] std::num::ParseIntError),
    #[error(transparent)]
    TooLarge(#[from] MaxSubmissionsTooLarge),
}

impl std::str::FromStr for MaxSubmissions {
    type Err = ParseMaxSubmissionsError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value: NonZero<u64> = s.parse()?;
        Ok(MaxSubmissions::new(value)?)
    }
}

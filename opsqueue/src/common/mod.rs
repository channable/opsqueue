//! Common datatypes and errors shared across all parts of Opsqueue
use rustc_hash::FxHashMap;

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

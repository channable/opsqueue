pub mod common;
pub mod consumer;
pub mod producer;
pub mod tracing;

#[cfg(feature = "client-logic")]
pub mod object_store;

#[cfg(feature = "server-logic")]
pub mod db;

#[cfg(feature = "server-logic")]
pub mod server;

#[cfg(feature = "server-logic")]
pub mod prometheus;

#[cfg(feature = "server-logic")]
pub mod config;

/// The Opsqueue library's semantic version
/// as written in the Rust packages's `Cargo.toml`
pub const VERSION_CARGO_SEMVER: &str = env!("CARGO_PKG_VERSION");

#[allow(clippy::const_is_empty)]
pub fn version_info() -> String {
    format!("v{VERSION_CARGO_SEMVER}")
}

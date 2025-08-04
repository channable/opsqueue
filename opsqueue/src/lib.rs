//! Opsqueue: A lightweight batch processing queue for heavy loads
//!
//! Simple 'getting started' instructions can be found [in the repository README](https://github.com/channable/opsqueue/).
//!
//! The Rust codebase defines both the 'server', which makes up the bulk of the Opsqueue binary itself,
//! and the 'client', which is the common part of functionality that clients written in different other programming languages
//! can all use.
//!
//! Many datatypes are shared between this server and client, and therefore their code lives together in the same crate.
//! Instead, we use feature-flags (`client-logic` and `server-logic`) to decide what concrete parts to include when building.
//! The set of dependencies is based on these same feature-flags.
//! Most interestingly, in the test suite we enable both feature-flags so we're able to do a bunch of round-trip testing
//! immediately in Rust code.
//!
//! # Module setup
//! - The basic logic is divided in the `producer` and `consumer` modules. These both have their own `db` submodule.
//! - Common functionality and datatypes exists in the `common` module
//! - Common database helpers live in the `db` module.
//! - Reading/writing to object stores like GCS or S3 is abstracted in the `object_store` module.
//! - Finally, extra modules to have a single source of truth for configuration of the queue, and to nicely do tracing and expose metrics exist.

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

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

/// The current git tag and/or git hash
/// as reported by `git describe`
///
/// Not always available; notably Nix removes the `.git` folder
/// before building the package.
pub const VERSION_GIT_HASH: &str = git_version::git_version!(
    args = ["--always", "--dirty=-modified", "--abbrev=40"],
    fallback = ""
);

#[allow(clippy::const_is_empty)]
pub fn version_info() -> String {
    if VERSION_GIT_HASH.is_empty() {
        format!("v{VERSION_CARGO_SEMVER}")
    } else {
        format!("v{VERSION_CARGO_SEMVER} (git: {VERSION_GIT_HASH})")
    }
}

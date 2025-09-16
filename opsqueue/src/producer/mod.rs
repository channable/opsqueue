//! The Producer side: Interface to create and read submissions

/// Client-specific code related to the [`Client`].
#[cfg(feature = "client-logic")]
pub mod client;

#[cfg(feature = "client-logic")]
pub use client::Client;

/// Server-specific code related to producers.
#[cfg(feature = "server-logic")]
pub mod server;

mod common;

pub use common::{ChunkContents, InsertSubmission};

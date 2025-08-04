//! The Consumer side: Interface to reserve, work on, and complete/fail individual Chunks
pub mod common;
pub mod strategy;

#[cfg(feature = "server-logic")]
pub mod dispatcher;

#[cfg(feature = "server-logic")]
pub mod server;

#[cfg(feature = "client-logic")]
pub mod client;

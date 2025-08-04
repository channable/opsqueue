//! The Producer side: Interface to create and read submissions
#[cfg(feature = "client-logic")]
pub mod client;
pub mod common;

#[cfg(feature = "server-logic")]
pub mod server;

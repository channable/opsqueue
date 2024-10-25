pub mod common;
pub mod consumer;
pub mod producer;

use std::future::IntoFuture;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::{stream, StreamExt, TryStreamExt};
use opsqueue::common::submission::Metadata;
use opsqueue::object_store::{ChunkType, ObjectStoreClient};
use pyo3::{create_exception, exceptions::PyException, prelude::*};

use opsqueue::common::{chunk, submission};
use opsqueue::consumer::client::OuterClient as ActualConsumerClient;
use opsqueue::consumer::strategy;



/// A Python module implemented in Rust.
#[pymodule]
fn opsqueue_internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // We want Rust logs created by code called from this module
    // to be forwarded to Python's logging system
    pyo3_log::init();

    // Classes
    m.add_class::<common::SubmissionId>()?;
    m.add_class::<common::ChunkIndex>()?;
    m.add_class::<common::Strategy>()?;
    m.add_class::<common::Chunk>()?;
    m.add_class::<common::SubmissionStatus>()?;
    m.add_class::<producer::PyChunksIter>()?;
    m.add_class::<consumer::ConsumerClient>()?;
    m.add_class::<producer::ProducerClient>()?;

    // Exception classes
    m.add(
        "ConsumerClientError",
        m.py().get_type_bound::<consumer::ConsumerClientError>(),
    )?;
    m.add(
        "ProducerClientError",
        m.py().get_type_bound::<producer::ProducerClientError>(),
    )?;

    // Top-level functions
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}

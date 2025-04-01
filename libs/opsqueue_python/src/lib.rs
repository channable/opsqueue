pub mod common;
pub mod consumer;
pub mod errors;
pub mod producer;

use pyo3::prelude::*;

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
    m.add_class::<common::ChunkFailed>()?;
    m.add_class::<common::SubmissionStatus>()?;
    m.add_class::<common::Submission>()?;
    m.add_class::<common::SubmissionFailed>()?;
    m.add_class::<producer::PyChunksIter>()?;
    m.add_class::<consumer::ConsumerClient>()?;
    m.add_class::<producer::ProducerClient>()?;

    // Exception classes
    m.add(
        "ConsumerClientError",
        m.py().get_type::<consumer::ConsumerClientError>(),
    )?;
    m.add(
        "ProducerClientError",
        m.py().get_type::<producer::ProducerClientError>(),
    )?;

    // Top-level functions
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}

use std::sync::Arc;

use pyo3::{exceptions::PyTypeError, prelude::*};

use toypsqueue::{common::submission::Metadata, producer::client::Client as ActualClient};

#[pyclass]
#[derive(Debug, Clone)]
struct Client{client: ActualClient, runtime: Arc<tokio::runtime::Runtime>}

#[pymethods]
impl Client {
    #[new]
    pub fn new(address: &str) -> Self {
        let runtime = start_runtime();
        let client = ActualClient::new(address);
        Client{client, runtime}
    }


    pub fn count_submissions(&self) -> PyResult<u32> {
        self.runtime.block_on(self.client.count_submissionns()).map_err(|e| PyTypeError::new_err(e.to_string()))
    }

    pub fn get_submission(&self, id: i64) -> PyResult<Option<String>> {
        self.runtime.block_on(self.client.get_submission(id))
        .map(|opt| opt.map(|submission_status| format!("{submission_status:?}")))
        .map_err(|e| PyTypeError::new_err(e.to_string()))
    }

    #[pyo3(signature = (directory_uri, chunk_count, metadata=None))]
    pub fn insert_submission(&self, directory_uri: String, chunk_count: u32, metadata: Option<Metadata>) -> PyResult<i64> {
        let submission = toypsqueue::producer::server::InsertSubmission {directory_uri, chunk_count, metadata};
        self.runtime.block_on(self.client.insert_submission(&submission))
        .map_err(|e| PyTypeError::new_err(e.to_string()))
    }
}

#[pyclass]
#[derive(Debug, Clone)]
struct InsertSubmission {
    #[pyo3(get, set)]
    pub directory_uri: String,
    #[pyo3(get, set)]
    pub chunk_count: u32,
    #[pyo3(get, set)]
    pub metadata: Option<Metadata>,
}

impl Into<toypsqueue::producer::server::InsertSubmission> for InsertSubmission {
    fn into(self) -> toypsqueue::producer::server::InsertSubmission {
        toypsqueue::producer::server::InsertSubmission {directory_uri: self.directory_uri, chunk_count: self.chunk_count, metadata: self.metadata}
    }
}

fn start_runtime() -> Arc<tokio::runtime::Runtime> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("Failed to create Tokio runtime in Toypsqueue Producer client");
    Arc::new(runtime)
}

// /// Formats the sum of two numbers as string.
// #[pyfunction]
// fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
//     Ok((a + b).to_string())
// }

/// A Python module implemented in Rust.
#[pymodule]
fn opsqueue_producer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<InsertSubmission>()?;
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}

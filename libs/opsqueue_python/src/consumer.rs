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

use crate::common::{run_unless_interrupted, start_runtime};

use super::common::{Chunk, ChunkIndex, Strategy, SubmissionId};


create_exception!(opsqueue_internal, ConsumerClientError, PyException);

#[pyclass]
#[derive(Debug)]
pub struct ConsumerClient {
    client: ActualConsumerClient,
    object_store_client: ObjectStoreClient,
    runtime: Arc<tokio::runtime::Runtime>,
}

fn maybe_wrap_error(e: anyhow::Error) -> PyErr {
    match e.downcast::<PyErr>() {
        Ok(py_err) => py_err,
        Err(other) => ConsumerClientError::new_err(other.to_string())
    }
}

#[pymethods]
impl ConsumerClient {
    #[new]
    pub fn new(address: &str, object_store_url: &str) -> PyResult<Self> {
        let runtime = start_runtime();
        let client = ActualConsumerClient::new(address);
        let object_store_client = ObjectStoreClient::new(object_store_url).map_err(maybe_wrap_error)?;
        log::info!("Opsqueue consumer client initialized");

        Ok(ConsumerClient {client, object_store_client, runtime })
    }

    pub fn reserve_chunks(&self, py: Python<'_>, max: usize, strategy: Strategy) -> PyResult<Vec<Chunk>> {
        py.allow_threads(|| { self.reserve_chunks_gilless(max, strategy)})
    }

    #[pyo3(signature = (submission_id, submission_prefix, chunk_index, output_content))]
    pub fn complete_chunk(
        &self,
        py: Python<'_>,
        submission_id: SubmissionId,
        submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        output_content: Vec<u8>,
    ) -> PyResult<()> {
        py.allow_threads(|| { self.complete_chunk_gilless(submission_id, submission_prefix, chunk_index, output_content)})
    }

    #[pyo3(signature = (submission_id, submission_prefix, chunk_index, failure))]
    pub fn fail_chunk(
        &self,
        py: Python<'_>,
        submission_id: SubmissionId,
        submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        failure: String,
    ) -> PyResult<()> {
        py.allow_threads(|| { self.fail_chunk_gilless(submission_id, submission_prefix, chunk_index, failure)})
    }

    pub fn run_per_chunk(&self, strategy: Strategy, fun: &Bound<'_, PyAny>) -> PyErr {
        if !fun.is_callable() {
            return pyo3::exceptions::PyTypeError::new_err("Expected `fun` parameter to be __call__-able");
        }
        // NOTE: We take care here to unlock the GIL for most of the loop,
        // and only re-lock it for the duration of each call to `fun`.
        let unbound_fun = fun.as_unbound();
        fun.py().allow_threads(|| {
            let mut done_count: usize = 0;
            loop {
                let chunk_outcome: PyResult<()> = (|| {
                    let chunks = self.reserve_chunks_gilless(1, strategy.clone())?;
                    log::debug!("Reserved {} chunks", chunks.len());
                    for chunk in chunks {
                        let submission_id = chunk.submission_id;
                        let submission_prefix = chunk.submission_prefix.clone();
                        let chunk_index = chunk.chunk_index;
                        log::debug!("Running fun for chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}", submission_id, chunk_index, &submission_prefix);
                        let res = Python::with_gil(|py| {
                            let res = unbound_fun.bind(py).call1((chunk,))?;
                            res.extract()
                        });
                        match res {
                            Ok(res) => {
                                log::debug!("Completing chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}", submission_id, chunk_index, &submission_prefix);
                                self.complete_chunk_gilless(submission_id, submission_prefix.clone(), chunk_index, res)?;
                                log::debug!("Completed chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}", submission_id, chunk_index, &submission_prefix);
                            },
                            Err(failure) => {
                                log::warn!("Failing chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}, reason: {failure:?}", submission_id, chunk_index, &submission_prefix);
                                self.fail_chunk_gilless(submission_id, submission_prefix.clone(), chunk_index, format!("{failure:?}"))?;
                                log::warn!("Failed chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}", submission_id, chunk_index, &submission_prefix);
                                // On exceptions that are not PyExceptions (but PyBaseExceptions), like KeyboardInterrupt etc, return.
                                // otherwise, continue with next chunk
                                if !Python::with_gil(|py| {failure.is_instance_of::<PyException>(py)}) {
                                    return Err(failure)
                                }
                            }
                        }

                        done_count = done_count.saturating_add(1);
                        if done_count % 50 == 0 {
                            log::info!("Processed {} chunks", done_count);
                        }
                    }
                    Ok(())
                })();

                // NOTE: We currently only quit on KeyboardInterrupt.
                // Any other error (like e.g. connection errors)
                // results in looping, which will re-establish the client connection.
                match chunk_outcome {
                    Ok(()) => {}
                    // In essence we 'catch `Exception` (but _not_ `BaseException` here)
                    Err(e) if Python::with_gil(|py| {e.is_instance_of::<PyException>(py)}) => {
                        log::warn!("Opsqueue consumer encountered an error, but will continue: {}", e);
                    }
                    Err(e) => {
                        log::info!("Opsqueue consumer closing because of exception: {e:?}");
                        return e
                    }
                }
            }
        })
    }

}

// What follows are internal helper functions
// that are not available directly from Python
impl ConsumerClient {
    fn block_unless_interrupted<T, E>(&self, future: impl IntoFuture<Output = Result<T, E>>) -> Result<T, E>
    where
    E: From<PyErr>,
    {
        self.runtime.block_on(run_unless_interrupted(future))

    }

    fn sleep_unless_interrupted<E>(&self, duration: Duration) -> Result<(), E>
    where
        E: From<PyErr>
    {
        self.block_unless_interrupted(async {
            tokio::time::sleep(duration).await;
            Ok(())
        })
    }


    fn reserve_chunks_gilless(&self, max: usize, strategy: Strategy) -> PyResult<Vec<Chunk>> {
        // TODO: Currently we do short-polling here if there are no chunks available.
        // This is quite suboptimal; long-polling would be much nicer.
        const POLL_INTERVAL: Duration = Duration::from_millis(500);
        let strategy: strategy::Strategy = strategy.into();
        loop {
            let res: Vec<Chunk> =
                self.block_unless_interrupted(self.reserve_and_retrieve_chunks(max, strategy.clone()))
                .map_err(maybe_wrap_error)?;
            if !res.is_empty() {
                return Ok(res);
            }
            self.sleep_unless_interrupted::<PyErr>(POLL_INTERVAL)?;
        }
    }

    fn complete_chunk_gilless(
        &self,
        submission_id: SubmissionId,
        submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        output_content: Vec<u8>,
    ) -> PyResult<()> {
        let chunk_id = (submission_id.into(), chunk_index.into());
        self.block_unless_interrupted(async move {
            match submission_prefix {
                None =>
                    self.client.complete_chunk(chunk_id, Some(output_content)).await,
                Some(prefix) => {
                    self.object_store_client.store_chunk(&prefix, chunk_id.1, ChunkType::Output, output_content).await?;
                    self.client.complete_chunk(chunk_id, None).await
                }
            }
    })
            .map_err(maybe_wrap_error)
    }

    pub fn fail_chunk_gilless(
        &self,
        submission_id: SubmissionId,
        _submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        failure: String,
    ) -> PyResult<()> {
        let chunk_id = (submission_id.into(), chunk_index.into());
        self.block_unless_interrupted(self.client.fail_chunk(chunk_id, failure))
            .map_err(maybe_wrap_error)
    }

    async fn reserve_and_retrieve_chunks(&self, max: usize, strategy: opsqueue::consumer::strategy::Strategy) -> anyhow::Result<Vec<Chunk>>{
        let chunks = self.client.reserve_chunks(max, strategy).await?;
        let elems =
            stream::iter(chunks)
            .then(|(c, s)| Chunk::from_internal(c, s, &self.object_store_client))
            .try_collect().await?;
        Ok(elems)
    }
}

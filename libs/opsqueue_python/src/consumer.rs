use std::future::IntoFuture;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream, StreamExt, TryStreamExt};
use opsqueue::{
    common::errors::{E::{self, L, R}, IncorrectUsage, LimitIsZero},
    consumer::client::InternalConsumerClientError,
    object_store::{ChunkRetrievalError, ChunkStorageError, ChunkType, NewObjectStoreClientError, ObjectStoreClient},
    E,
};
use pyo3::{create_exception, exceptions::PyException, prelude::*};

use opsqueue::consumer::client::OuterClient as ActualConsumerClient;
use opsqueue::consumer::strategy;

use crate::errors::{CError, CPyResult};
use crate::{
    common::{run_unless_interrupted, start_runtime},
    errors::FatalPythonException,
};

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
        Err(other) => ConsumerClientError::new_err(other.to_string()),
    }
}

#[pymethods]
impl ConsumerClient {
    #[new]
    pub fn new(
        address: &str,
        object_store_url: &str,
    ) -> CPyResult<Self, NewObjectStoreClientError> {
        let runtime = start_runtime();
        let client = ActualConsumerClient::new(address);
        let object_store_client = ObjectStoreClient::new(object_store_url).map_err(CError)?;
        log::info!("Opsqueue consumer client initialized");

        Ok(ConsumerClient {
            client,
            object_store_client,
            runtime,
        })
    }

    pub fn reserve_chunks(
        &self,
        py: Python<'_>,
        max: usize,
        strategy: Strategy,
    ) -> CPyResult<
        Vec<Chunk>,
        E![
            FatalPythonException,
            ChunkRetrievalError,
            InternalConsumerClientError,
            IncorrectUsage<LimitIsZero>,
        ],
    > {
        py.allow_threads(|| self.reserve_chunks_gilless(max, strategy))
    }

    #[pyo3(signature = (submission_id, submission_prefix, chunk_index, output_content))]
    pub fn complete_chunk(
        &self,
        py: Python<'_>,
        submission_id: SubmissionId,
        submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        output_content: Vec<u8>,
    ) -> CPyResult<(), 
        E![
            FatalPythonException, 
            ChunkStorageError, 
            InternalConsumerClientError]
        > {
        py.allow_threads(|| {
            self.complete_chunk_gilless(
                submission_id,
                submission_prefix,
                chunk_index,
                output_content,
            )
        })
    }

    #[pyo3(signature = (submission_id, submission_prefix, chunk_index, failure))]
    pub fn fail_chunk(
        &self,
        py: Python<'_>,
        submission_id: SubmissionId,
        submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        failure: String,
    ) -> CPyResult<(), E<FatalPythonException, InternalConsumerClientError>> {
        py.allow_threads(|| {
            self.fail_chunk_gilless(submission_id, submission_prefix, chunk_index, failure)
        })
    }

    pub fn run_per_chunk(
        &self,
        strategy: Strategy,
        fun: &Bound<'_, PyAny>,
    ) -> CError<
        E![
            FatalPythonException,
            ChunkStorageError,
            ChunkRetrievalError,
            InternalConsumerClientError,
            IncorrectUsage<LimitIsZero>,
        ],
    > {
        if !fun.is_callable() {
            return pyo3::exceptions::PyTypeError::new_err(
                "Expected `fun` parameter to be __call__-able",
            )
            .into();
        }
        // NOTE: We take care here to unlock the GIL for most of the loop,
        // and only re-lock it for the duration of each call to `fun`.
        let unbound_fun = fun.as_unbound();
        fun.py().allow_threads(|| {
            let mut done_count: usize = 0;
            loop {
                let chunk_outcome: CPyResult<(), E![FatalPythonException, ChunkStorageError, ChunkRetrievalError, InternalConsumerClientError, IncorrectUsage<LimitIsZero>]> = (|| {
                    let chunks = self.reserve_chunks_gilless(1, strategy.clone()).map_err(|e| match e {
                        CError(L(e)) => CError(L(e)),
                        CError(R(L(e))) => CError(R(R(L(e)))),
                        CError(R(R(e))) => CError(R(R(R(e)))),
                    })?;
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
                                self.complete_chunk_gilless(submission_id, submission_prefix.clone(), chunk_index, res).map_err(|e| match e {
                                    CError(L(e)) => CError(L(e)),
                                    CError(R(L(e))) => CError(R(L(e))),
                                    CError(R(R(e))) => CError(R(R(R(L(e)))))
                                })?;
                                log::debug!("Completed chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}", submission_id, chunk_index, &submission_prefix);
                            },
                            Err(failure) => {
                                log::warn!("Failing chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}, reason: {failure:?}", submission_id, chunk_index, &submission_prefix);
                                self.fail_chunk_gilless(submission_id, submission_prefix.clone(), chunk_index, format!("{failure:?}")).map_err(|e| 
                                    match e {
                                        CError(L(py_err)) => CError(L(py_err)),
                                        CError(R(e)) => CError(R(R(R(L(e))))),
                                    }

                                )?;
                                log::warn!("Failed chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}", submission_id, chunk_index, &submission_prefix);
                                // On exceptions that are not PyExceptions (but PyBaseExceptions), like KeyboardInterrupt etc, return.
                                // otherwise, continue with next chunk
                                if !Python::with_gil(|py| {failure.is_instance_of::<PyException>(py)}) {
                                    return Err(failure.into())
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
                    Err(CError(L(e))) => {
                        log::info!("Opsqueue consumer closing because of exception: {e:?}");
                        return CError(L(e))
                    }
                    Err(CError(R(err))) => {
                        log::warn!("Opsqueue consumer encountered a Rust error, but will continue: {}", err);
                    }
                }
            }
        })
    }
}


// What follows are internal helper functions
// that are not available directly from Python
impl ConsumerClient {
    fn block_unless_interrupted<T, E>(
        &self,
        future: impl IntoFuture<Output = Result<T, E>>,
    ) -> Result<T, E>
    where
        E: From<FatalPythonException>,
    {
        self.runtime.block_on(run_unless_interrupted(future))
    }

    fn sleep_unless_interrupted<E>(&self, duration: Duration) -> Result<(), E>
    where
        E: From<FatalPythonException>,
    {
        self.block_unless_interrupted(async {
            tokio::time::sleep(duration).await;
            Ok(())
        })
    }

    fn reserve_chunks_gilless(
        &self,
        max: usize,
        strategy: Strategy,
    ) -> CPyResult<
        Vec<Chunk>,
        E<
            FatalPythonException,
            E<
                ChunkRetrievalError,
                E<InternalConsumerClientError, IncorrectUsage<LimitIsZero>>,
            >,
        >,
    > {
        // TODO: Currently we do short-polling here if there are no chunks available.
        // This is quite suboptimal; long-polling would be much nicer.
        const POLL_INTERVAL: Duration = Duration::from_millis(500);
        let strategy: strategy::Strategy = strategy.into();
        loop {
            let res = self.block_unless_interrupted(async {
                self.reserve_and_retrieve_chunks(max, strategy.clone())
                    .await
                    .map_err(|e| CError(R(e.into())))
            });
            match res {
                Err(e) => return Err(e),
                Ok(chunks) if chunks.is_empty() => {
                    self.sleep_unless_interrupted::<FatalPythonException>(POLL_INTERVAL)?
                }
                Ok(chunks) => return Ok(chunks),
            }
        }
    }

    fn complete_chunk_gilless(
        &self,
        submission_id: SubmissionId,
        submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        output_content: Vec<u8>,
    ) -> CPyResult<(), 
        E![
            FatalPythonException, 
            ChunkStorageError, 
            InternalConsumerClientError]
        > 
        {
        let chunk_id = (submission_id.into(), chunk_index.into());
        self.block_unless_interrupted(async move {
            match submission_prefix {
                None => {
                    self.client
                        .complete_chunk(chunk_id, Some(output_content))
                        .await.map_err(|e| CError(R(R(e))))
                }
                Some(prefix) => {
                    self.object_store_client
                        .store_chunk(&prefix, chunk_id.1, ChunkType::Output, output_content)
                        .await.map_err(|e| CError(R(L(e))))?;
                    self.client.complete_chunk(chunk_id, None).await.map_err(|e| CError(R(R(e))))
                }
            }
        })
    }

    pub fn fail_chunk_gilless(
        &self,
        submission_id: SubmissionId,
        _submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        failure: String,
    ) -> CPyResult<(), E<FatalPythonException, InternalConsumerClientError>> {
        let chunk_id = (submission_id.into(), chunk_index.into());
        self.block_unless_interrupted(async {
            self.client
                .fail_chunk(chunk_id, failure)
                .await
                .map_err(R)
                .map_err(CError)
        })
    }

    async fn reserve_and_retrieve_chunks(
        &self,
        max: usize,
        strategy: opsqueue::consumer::strategy::Strategy,
    ) -> Result<
        Vec<Chunk>,
        E<
            ChunkRetrievalError,
            E<InternalConsumerClientError, IncorrectUsage<LimitIsZero>>,
        >,
    > {
        let chunks = self.client.reserve_chunks(max, strategy).await?;
        stream::iter(chunks)
            .then(|(c, s)| Chunk::from_internal(c, s, &self.object_store_client))
            .try_collect()
            .await
            .map_err(|e| L(e))
    }
}

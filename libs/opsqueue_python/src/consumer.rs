use std::future::IntoFuture;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream, StreamExt, TryStreamExt};
use opsqueue::{
    common::errors::{
        IncorrectUsage, LimitIsZero,
        E::{self, L, R},
    },
    consumer::client::InternalConsumerClientError,
    object_store::{
        ChunkRetrievalError, ChunkStorageError, ChunkType, NewObjectStoreClientError,
        ObjectStoreClient,
    },
    E,
};
use pyo3::{
    create_exception,
    exceptions::{PyException, PySystemError},
    prelude::*,
};

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

#[pymethods]
impl ConsumerClient {
    /// Create a new client instance.
    ///
    /// :param address: The HTTP address where the opsqueue instance is running.
    ///
    /// :param object_store_url: The URL used to upload/download objects from e.g. GCS.
    ///   use `file:///tmp/my/local/path` to use a local file when running small examples in development.
    ///   use `gs://bucket-name/path/inside/bucket` to connect to GCS in production.
    ///   Supports the formats listed here: https://docs.rs/object_store/0.11.1/object_store/enum.ObjectStoreScheme.html#method.parse
    /// :param object_store_options: A list of key-value strings as extra options for the chosen object store.
    ///        For example, for GCS, see https://docs.rs/object_store/0.11.2/object_store/gcp/enum.GoogleConfigKey.html#variants
    #[new]
    #[pyo3(signature = (address, object_store_url, object_store_options=vec![]))]
    pub fn new(
        address: &str,
        object_store_url: &str,
        object_store_options: Vec<(String, String)>,
    ) -> CPyResult<Self, NewObjectStoreClientError> {
        tracing::info!(
            "Initializing Opsqueue ConsumerClient (Opsqueue version: {})",
            opsqueue::version_info()
        );
        let runtime = start_runtime();
        let client = ActualConsumerClient::new(address);
        let object_store_client =
            ObjectStoreClient::new(object_store_url, object_store_options).map_err(CError)?;
        tracing::info!("Opsqueue ConsumerClient initialized");

        Ok(ConsumerClient {
            client,
            object_store_client,
            runtime,
        })
    }

    pub fn __repr__(&self) -> String {
        format!(
            "<opsqueue_producer.ConsumerClient(address={:?}, object_store_url={:?})>",
            self.client.address(),
            self.object_store_client.url()
        )
    }

    #[allow(clippy::type_complexity)]
    pub fn reserve_chunks(
        &self,
        py: Python<'_>,
        max: usize,
        strategy: &Strategy,
    ) -> CPyResult<
        Vec<Chunk>,
        E![
            FatalPythonException,
            ChunkRetrievalError,
            InternalConsumerClientError,
            IncorrectUsage<LimitIsZero>,
        ],
    > {
        py.allow_threads(|| self.reserve_chunks_gilless(max, strategy.into()))
    }

    #[pyo3(signature = (submission_id, submission_prefix, chunk_index, output_content))]
    pub fn complete_chunk(
        &self,
        py: Python<'_>,
        submission_id: SubmissionId,
        submission_prefix: Option<String>,
        chunk_index: ChunkIndex,
        output_content: Vec<u8>,
    ) -> CPyResult<
        (),
        E![
            FatalPythonException,
            ChunkStorageError,
            InternalConsumerClientError
        ],
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

    #[allow(clippy::type_complexity)]
    pub fn run_per_chunk(
        &self,
        strategy: &Strategy,
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
                let chunk_outcome = self.compute_chunk_outcome(strategy, unbound_fun, done_count);
                // NOTE: We currently only quit on KeyboardInterrupt.
                // Any other error (like e.g. connection errors)
                // results in looping, which will re-establish the client connection.
                match chunk_outcome {
                    Ok(done) => {
                        done_count = done_count.saturating_add(done);
                    }
                    // In essence we 'catch `Exception` (but _not_ `BaseException` here)
                    Err(CError(L(e))) => {
                        tracing::info!("Opsqueue consumer closing because of exception: {e:?}");
                        return CError(L(e));
                    }
                    Err(CError(R(err))) => {
                        tracing::warn!(
                            "Opsqueue consumer encountered a Rust error, but will continue: {}",
                            err
                        );
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

    #[allow(clippy::type_complexity)]
    fn reserve_chunks_gilless(
        &self,
        max: usize,
        strategy: strategy::Strategy,
    ) -> CPyResult<
        Vec<Chunk>,
        E<
            FatalPythonException,
            E<ChunkRetrievalError, E<InternalConsumerClientError, IncorrectUsage<LimitIsZero>>>,
        >,
    > {
        const POLL_INTERVAL: Duration = Duration::from_millis(500);
        loop {
            let res = self.block_unless_interrupted(async {
                self.reserve_and_retrieve_chunks(max, strategy.clone())
                    .await
                    .map_err(|e| CError(R(e)))
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
    ) -> CPyResult<
        (),
        E![
            FatalPythonException,
            ChunkStorageError,
            InternalConsumerClientError
        ],
    > {
        let chunk_id = (submission_id.into(), chunk_index.into()).into();
        self.block_unless_interrupted(async move {
            match submission_prefix {
                None => self
                    .client
                    .complete_chunk(chunk_id, Some(output_content))
                    .await
                    .map_err(|e| CError(R(R(e)))),
                Some(prefix) => {
                    self.object_store_client
                        .store_chunk(
                            &prefix,
                            chunk_id.chunk_index,
                            ChunkType::Output,
                            output_content,
                        )
                        .await
                        .map_err(|e| CError(R(L(e))))?;
                    self.client
                        .complete_chunk(chunk_id, None)
                        .await
                        .map_err(|e| CError(R(R(e))))
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
        let chunk_id = (submission_id.into(), chunk_index.into()).into();
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
        E<ChunkRetrievalError, E<InternalConsumerClientError, IncorrectUsage<LimitIsZero>>>,
    > {
        let chunks = self.client.reserve_chunks(max, strategy).await?;
        stream::iter(chunks)
            .then(|(c, s)| Chunk::from_internal(c, s, &self.object_store_client))
            .try_collect()
            .await
            .map_err(L)
    }

    #[allow(clippy::type_complexity)]
    fn compute_chunk_outcome(
        &self,
        strategy: &Strategy,
        unbound_fun: &Py<PyAny>,
        mut done_count: usize,
    ) -> CPyResult<
        usize,
        E![
            FatalPythonException,
            ChunkStorageError,
            ChunkRetrievalError,
            InternalConsumerClientError,
            IncorrectUsage<LimitIsZero>
        ],
    > {
        let chunks = self
            .reserve_chunks_gilless(1, strategy.into())
            .map_err(|e| match e {
                CError(L(e)) => CError(L(e)),
                CError(R(L(e))) => CError(R(R(L(e)))),
                CError(R(R(e))) => CError(R(R(R(e)))),
            })?;
        tracing::debug!("Reserved {} chunks", chunks.len());
        for chunk in chunks {
            let submission_id = chunk.submission_id;
            let submission_prefix = chunk.submission_prefix.clone();
            let chunk_index = chunk.chunk_index;
            tracing::debug!(
            "Running fun for chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}",
            submission_id,
            chunk_index,
            &submission_prefix
        );
            let res = Python::with_gil(|py| {
                let res = unbound_fun.bind(py).call1((chunk,))?;
                res.extract()
            });
            match res {
                Ok(res) => {
                    tracing::debug!("Completing chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}", submission_id, chunk_index, &submission_prefix);
                    self.complete_chunk_gilless(
                        submission_id,
                        submission_prefix.clone(),
                        chunk_index,
                        res,
                    )
                    .map_err(|e| match e {
                        CError(L(e)) => CError(L(e)),
                        CError(R(L(e))) => CError(R(L(e))),
                        CError(R(R(e))) => CError(R(R(R(L(e))))),
                    })?;
                    tracing::debug!(
                    "Completed chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}",
                    submission_id,
                    chunk_index,
                    &submission_prefix
                );
                }
                Err(failure) => {
                    let failure_str = crate::common::format_pyerr(&failure);
                    tracing::warn!("Failing chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}, reason: {failure_str}", submission_id, chunk_index, &submission_prefix);
                    self.fail_chunk_gilless(
                        submission_id,
                        submission_prefix.clone(),
                        chunk_index,
                        failure_str,
                    )
                    .map_err(|e| match e {
                        CError(L(py_err)) => CError(L(py_err)),
                        CError(R(e)) => CError(R(R(R(L(e))))),
                    })?;
                    tracing::warn!(
                    "Failed chunk: submission_id={:?}, chunk_index={:?}, submission_prefix={:?}",
                    submission_id,
                    chunk_index,
                    &submission_prefix
                );

                    // On exceptions that are not PyExceptions (but PyBaseExceptions), like KeyboardInterrupt etc, return.
                    if !Python::with_gil(|py| failure.is_instance_of::<PyException>(py)) {
                        return Err(failure.into());
                    }

                    // Exit also on SystemError
                    // as this might be thrown when someone tries to Ctrl-C
                    // while some native code (ex: OpenTelemetry integration)
                    // is executing.
                    if Python::with_gil(|py| failure.is_instance_of::<PySystemError>(py)) {
                        return Err(failure.into());
                    }

                    // otherwise, continue with next chunk
                }
            }

            done_count = done_count.saturating_add(1);
            if done_count % 50 == 0 {
                tracing::info!("Processed {} chunks", done_count);
            }
        }
        Ok(done_count)
    }
}

use std::{future::IntoFuture, sync::Arc, time::Duration};

use pyo3::{create_exception, exceptions::PyException, prelude::*, types::PyIterator};

use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use opsqueue::{
    common::{chunk, submission},
    object_store::{ChunkRetrievalError, ChunkType},
    producer::common::ChunkContents,
    E,
};
use opsqueue::{
    common::{errors::E::{self, L, R}, NonZero, NonZeroIsZero},
    object_store::{ChunksStorageError, NewObjectStoreClientError},
    producer::client::{Client as ActualClient, InternalProducerClientError},
};
use tokio::sync::Mutex;
use ux_serde::u63;

use crate::{
    common::{run_unless_interrupted, start_runtime, SubmissionId, SubmissionStatus, VecAsPyBytes},
    errors::{CError, CPyResult, FatalPythonException},
};

create_exception!(opsqueue_internal, ProducerClientError, PyException);

const SUBMISSION_POLLING_INTERVAL: Duration = Duration::from_secs(1);

// NOTE: ProducerClient is reasonably cheap to clone, as most of its fields are behind Arcs.
#[pyclass]
#[derive(Debug, Clone)]
pub struct ProducerClient {
    producer_client: ActualClient,
    object_store_client: opsqueue::object_store::ObjectStoreClient,
    runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl ProducerClient {
    /// Create a new client instance.
    ///
    /// :param address: The HTTP address where the opsqueue instance is running.
    ///
    /// :param object_store_url: The URL used to upload/download objects from e.g. GCS.
    ///   use `file:///tmp/my/local/path` to use a local file when running small examples in development.
    ///   use `gs://bucket-name/path/inside/bucket` to connect to GCS in production.
    ///   Supports the formats listed here: https://docs.rs/object_store/0.11.1/object_store/enum.ObjectStoreScheme.html#method.parse
    ///   Note that other GCS settings are read from environment variables, using the steps outlined here: https://cloud.google.com/docs/authentication/application-default-credentials.
    #[new]
    pub fn new(
        address: &str,
        object_store_url: &str,
    ) -> CPyResult<Self, NewObjectStoreClientError> {
        let runtime = start_runtime();
        let producer_client = ActualClient::new(address);
        let object_store_client = opsqueue::object_store::ObjectStoreClient::new(object_store_url)?;
        Ok(ProducerClient {
            producer_client,
            object_store_client,
            runtime,
        })
    }

    pub fn __repr__(&self) -> String {
        format!(
            "<opsqueue_producer.ProducerClient(address={:?}, object_store_url={:?})>",
            self.producer_client.endpoint_url, self.object_store_client.url()
        )
    }

    /// Counts the number of ongoing submissions in the queue.
    ///
    /// Completed and failed submissions are not included in the count.
    pub fn count_submissions(
        &self,
        py: Python<'_>,
    ) -> CPyResult<u32, E<FatalPythonException, InternalProducerClientError>> {
        py.allow_threads(|| {
            self.block_unless_interrupted(async {
                self.producer_client
                    .count_submissions()
                    .await
                    .map_err(|e| CError(R(e)))
            })
        })
    }

    /// Retrieve the status (in progress, completed or failed) of a specific submission.
    ///
    /// The returned SubmissionStatus object also includes the number of chunks finished so far,
    /// when the submission was started/completed/failed, etc.
    ///
    /// This call does _not_ fetch the submission's chunk contents on its own.
    pub fn get_submission_status(
        &self,
        py: Python<'_>,
        id: SubmissionId,
    ) -> CPyResult<
        Option<SubmissionStatus>,
        E<FatalPythonException, InternalProducerClientError>,
    > {
        py.allow_threads(|| {
            self.block_unless_interrupted(async {
                self.producer_client
                    .get_submission(id.into())
                    .await
                    .map_err(|e| CError(R(e)))
            })
            .map(|opt| opt.map(Into::into))
            // .map_err(|e| ProducerClientError::new_err(e.to_string()))
        })
    }

    #[pyo3(signature = (chunk_contents, metadata=None))]
    pub fn insert_submission_direct(
        &self,
        py: Python<'_>,
        chunk_contents: Vec<chunk::Content>,
        metadata: Option<submission::Metadata>,
    ) -> CPyResult<SubmissionId, E<FatalPythonException, InternalProducerClientError>> {
        py.allow_threads(|| {
            let submission = opsqueue::producer::common::InsertSubmission {
                chunk_contents: ChunkContents::Direct {
                    contents: chunk_contents,
                },
                metadata,
            };
            self.block_unless_interrupted(async move {
                self.producer_client
                    .insert_submission(&submission)
                    .await
                    .map_err(|e| R(e).into())
            })
            .map(Into::into)
        })
    }

    #[pyo3(signature = (chunk_contents, metadata=None))]
    #[allow(clippy::type_complexity)]
    pub fn insert_submission_chunks(
        &self,
        py: Python<'_>,
        chunk_contents: Py<PyIterator>,
        metadata: Option<submission::Metadata>,
    ) -> CPyResult<
        SubmissionId,
        E![
            FatalPythonException,
            NonZeroIsZero<chunk::ChunkIndex>,
            ChunksStorageError,
            InternalProducerClientError,
        ],
    > {
        // This function is split into two parts.
        // For the upload to object storage, we need the GIL as we run the python iterator to completion.
        // For the second part, where we send the submission to the queue, we no longer need the GIL (and unlock it to allow logging later).
        py.allow_threads(|| {
            let prefix = uuid::Uuid::new_v4().to_string();
            let chunk_count = Python::with_gil(|py| {
                self.block_unless_interrupted(async {
                    let chunk_contents = chunk_contents.bind(py);
                    let stream = futures::stream::iter(chunk_contents)
                        .map(|item| item.and_then(|item| item.extract()).map_err(Into::into));
                    self.object_store_client
                        .store_chunks(&prefix, ChunkType::Input, stream)
                        .await
                        .map_err(|e| CError(R(R(L(e)))))
                })
            })?;
            let chunk_count = NonZero::try_from(chunk::ChunkIndex::from(chunk_count))
                .map_err(|e| R(L(e)))?;

            self.block_unless_interrupted(async move {
                let submission = opsqueue::producer::common::InsertSubmission {
                    chunk_contents: ChunkContents::SeeObjectStorage {
                        prefix,
                        count: chunk_count,
                    },
                    metadata,
                };
                self.producer_client
                    .insert_submission(&submission)
                    .await
                    .map(|submission_id| submission_id.into())
                    .map_err(|e| R(R(R(e))).into())
            })
        })
    }

    pub fn stream_completed_submission(
        &self,
        py: Python<'_>,
        id: SubmissionId,
    ) -> CPyResult<
        PyChunksIter,
        E![
            FatalPythonException,
            SubmissionNotCompletedYetError,
            InternalProducerClientError,
        ],
    > {
        // TODO: Use CPyResult instead
        py.allow_threads(|| {
            self.block_unless_interrupted(async move {
                match self
                    .maybe_stream_completed_submission(id)
                    .await
                    .map_err(|CError(e)| CError(R(R(e))))?
                {
                    None => Err(CError(R(L(
                        SubmissionNotCompletedYetError(id),
                    ))))?,
                    Some(py_iter) => Ok(py_iter),
                }
            })
        })
    }

    #[pyo3(signature = (chunk_contents, metadata=None))]
    #[allow(clippy::type_complexity)]
    pub fn run_submission_chunks(
        &self,
        py: Python<'_>,
        chunk_contents: Py<PyIterator>,
        metadata: Option<submission::Metadata>,
    ) -> CPyResult<
        PyChunksIter,
        E![
            FatalPythonException,
            NonZeroIsZero<chunk::ChunkIndex>,
            ChunksStorageError,
            InternalProducerClientError,
        ],
    > {
        let submission_id = self.insert_submission_chunks(py, chunk_contents, metadata)?;
        py.allow_threads(|| {
            self.block_unless_interrupted(async move {
                loop {
                    if let Some(py_stream) = self
                        .maybe_stream_completed_submission(submission_id)
                        .await
                        .map_err(|CError(e)| {
                            CError(R(R(R(e))))
                        })?
                    {
                        return Ok(py_stream);
                    }
                    tokio::time::sleep(SUBMISSION_POLLING_INTERVAL).await;
                }
            })
        })
    }
}

#[derive(thiserror::Error, Debug)]
#[error("The submission with ID {0:?} is not completed yet. ")]
pub struct SubmissionNotCompletedYetError(pub SubmissionId);

// What follows are internal helper functions
// that are not available from Python
impl ProducerClient {
    fn block_unless_interrupted<T, E>(
        &self,
        future: impl IntoFuture<Output = Result<T, E>>,
    ) -> Result<T, E>
    where
        E: From<FatalPythonException>,
    {
        self.runtime.block_on(run_unless_interrupted(future))
    }

    async fn maybe_stream_completed_submission(
        &self,
        id: SubmissionId,
    ) -> CPyResult<Option<PyChunksIter>, InternalProducerClientError> {
        match self.producer_client.get_submission(id.into()).await? {
            Some(submission::SubmissionStatus::Completed(submission)) => {
                let prefix = submission.prefix.unwrap_or_default();
                let py_chunks_iter =
                    PyChunksIter::new(self.clone(), prefix, submission.chunks_total).await;

                Ok(Some(py_chunks_iter))
            }
            _ => Ok(None),
        }
    }
}

#[pyclass]
pub struct PyChunksIter {
    stream: Mutex<BoxStream<'static, CPyResult<Vec<u8>, ChunkRetrievalError>>>,
    producer_client: ProducerClient,
}

impl PyChunksIter {
    pub(crate) async fn new(client: ProducerClient, prefix: String, chunks_total: u63) -> Self {
        let stream = client.object_store_client.retrieve_chunks(&prefix, chunks_total, ChunkType::Output).await.map_err(CError).boxed();
        Self {stream: Mutex::new(stream), producer_client: client}
    }
}

#[pymethods]
impl PyChunksIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRefMut<'_, Self>) -> Option<CPyResult<VecAsPyBytes, ChunkRetrievalError>> {
        slf.producer_client.runtime.block_on(async {
            let mut stream = slf.stream.lock().await;
            stream.next().await.map(|r| r.map(VecAsPyBytes))
        })
    }
}

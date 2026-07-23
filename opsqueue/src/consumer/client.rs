use std::{
    collections::HashMap,
    error::Error,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwapOption;
use futures::{SinkExt, Stream, StreamExt, stream::SplitSink};
use http::Uri;
use tokio::{net::TcpStream, sync::oneshot::error::RecvError};
use tokio::{
    select,
    sync::{Mutex, oneshot},
    task::yield_now,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{self, Message},
};
use tokio_util::sync::{CancellationToken, DropGuard};
// use tokio_websockets::{MaybeTlsStream, Message, WebSocketStream};

use crate::{
    common::{
        chunk::{self, Chunk, ChunkId},
        errors::{E, IncorrectUsage, LimitIsZero},
        submission::Submission,
    },
    consumer::common::{AsyncServerToClientMessage, Envelope},
};

use super::{
    common::{
        ClientToServerMessage, ConsumerConfig, ServerToClientMessage, SyncServerToClientResponse,
    },
    strategy::Strategy,
};

use backon::{BackoffBuilder, FibonacciBuilder, Retryable};

type WebsocketTcpStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A wrapper around the actual client,
/// ensuring that the client:
/// - Is initialized lazily
/// - Is reset on low-level failures
/// - And therefore, that it is resilient to temporary network failures
#[derive(Debug)]
pub struct OuterClient(ArcSwapOption<Client>, Box<str>);

impl OuterClient {
    /// Construct a lazy, reconnecting consumer client.
    #[must_use]
    pub fn new(url: &str) -> Self {
        Self(None.into(), url.into())
    }

    pub fn address(&self) -> &str {
        &self.1
    }

    /// Reserve up to `max` chunks from the server.
    ///
    /// # Panics
    ///
    /// Panics if internal lazy initialization invariants are violated.
    ///
    /// # Errors
    ///
    /// Returns an error if input usage is invalid, or if websocket/serialization
    /// communication with the server fails.
    pub async fn reserve_chunks(
        &self,
        max: usize,
        strategy: Strategy,
    ) -> Result<Vec<(Chunk, Submission)>, E<InternalConsumerClientError, IncorrectUsage<LimitIsZero>>>
    {
        self.ensure_initialized().await;
        let res = self
            .0
            .load()
            .as_ref()
            .expect("Should always be initialized after `.ensure_initialized()")
            .reserve_chunks(max, strategy)
            .await;
        if res.is_err() {
            self.0.store(None);
        }
        res
    }

    /// Mark a chunk as completed.
    ///
    /// # Panics
    ///
    /// Panics if internal lazy initialization invariants are violated.
    ///
    /// # Errors
    ///
    /// Returns an error if websocket communication with the server fails.
    pub async fn complete_chunk(
        &self,
        id: ChunkId,
        output_content: chunk::Content,
    ) -> Result<(), InternalConsumerClientError> {
        self.ensure_initialized().await;
        let res = self
            .0
            .load()
            .as_ref()
            .expect("Should always be initialized after `.ensure_initialized()")
            .complete_chunk(id, output_content)
            .await;
        if res.is_err() {
            self.0.store(None);
        }
        res
    }

    /// Mark a chunk as failed.
    ///
    /// # Panics
    ///
    /// Panics if internal lazy initialization invariants are violated.
    ///
    /// # Errors
    ///
    /// Returns an error if websocket communication with the server fails.
    pub async fn fail_chunk(
        &self,
        id: ChunkId,
        failure: String,
    ) -> Result<(), InternalConsumerClientError> {
        self.ensure_initialized().await;
        let res = self
            .0
            .load()
            .as_ref()
            .expect("Should always be initialized after `.ensure_initialized()")
            .fail_chunk(id, failure)
            .await;
        if res.is_err() {
            self.0.store(None);
        }
        res
    }

    async fn ensure_initialized(&self) {
        if !self.is_healthy() {
            let client = self.initialize().await;
            self.0.store(Some(Arc::new(client)));
        }
    }

    async fn initialize(&self) -> Client {
        tracing::info!("Initializing (or re-initializing) consumer client connection...");
        (|| Client::new(&self.1))
        .retry(retry_policy())
        .notify(|err, duration| { tracing::debug!("Error establishing consumer client WS connection. (Will retry in {duration:?}). Details: {err:?}") })
        .await
        .expect("Infinite retries should never return Err")
    }

    /// When `false` is returned, the next call to the client will attempt to restore the connection.
    ///
    /// This function can be used to propagate healthiness info to a consumer service.
    pub fn is_healthy(&self) -> bool {
        let inner = self.0.load();
        inner.as_ref().is_some_and(|c| c.is_healthy())
    }
}

// TODO: Set max retries to `None`;
// will require either writing our own Backoff (iterator)
// or extending the backon crate.
fn retry_policy() -> impl BackoffBuilder {
    FibonacciBuilder::default()
        .with_jitter()
        .with_min_delay(Duration::from_millis(10))
        .with_max_delay(Duration::from_secs(5))
        .without_max_times()
}

/// A shared tuple of:
///
/// * A nonce counter
/// * A map of nonce -> oneshot sender for the response of the request
///
/// Asynchronous requests are sent to the server with a nonce but we don't expect a response for
/// them.
///
/// Synchronous requests are sent to the server with a nonce and we expect a response for them. The
/// nonce is used to match the response to the request.
#[allow(clippy::type_complexity)]
#[derive(Debug, Clone)]
struct InFlightRequests(
    Arc<(
        AtomicUsize,
        Mutex<HashMap<usize, oneshot::Sender<SyncServerToClientResponse>>>,
    )>,
);

impl InFlightRequests {
    fn next_nonce(&self) -> usize {
        self.0.0.fetch_add(1, Ordering::SeqCst)
    }

    async fn next_nonce_with_oneshot(
        &self,
    ) -> (usize, oneshot::Receiver<SyncServerToClientResponse>) {
        let (oneshot_sender, oneshot_receiver) = oneshot::channel();
        let mut guard = self.0.1.lock().await;
        // This is called within the lock, so we know the nonce and the oneshot_sender are inserted atomically.
        let nonce = self.next_nonce();
        guard.insert(nonce, oneshot_sender);
        (nonce, oneshot_receiver)
    }

    async fn send(
        &self,
        envelop: Envelope<SyncServerToClientResponse>,
    ) -> Result<(), SyncServerToClientResponse> {
        let mut in_flight_requests = self.0.1.lock().await;
        let oneshot_sender = in_flight_requests
            .remove(&envelop.nonce)
            .expect("Received response with nonce that matches none of the open requests");
        oneshot_sender.send(envelop.contents)
    }

    async fn clear(&self) {
        let mut in_flight_requests = self.0.1.lock().await;
        // This is called within the lock, so we know the nonce and the `onceshot_sender`s are cleared atomically.
        self.0.0.store(0, Ordering::SeqCst);
        in_flight_requests.clear();
    }
}

#[derive(Debug)]
pub struct Client {
    in_flight_requests: InFlightRequests,
    ws_sink: Arc<Mutex<SplitSink<WebsocketTcpStream, Message>>>,
    healthy: Arc<AtomicBool>,
    _bg_handle: DropGuard,
}

impl Client {
    /// Construct and initialize a websocket consumer client.
    ///
    /// # Errors
    ///
    /// Returns an error if URL parsing fails, websocket connection setup fails,
    /// or the initial server message is invalid.
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        // Ensure that the given URL is always a websocket URL; tungstenite requires this
        let websocket_url = if url.starts_with("ws://") || url.starts_with("wss://") {
            format!("{url}/consumer")
        } else {
            format!("ws://{url}/consumer")
        };
        let endpoint_uri = Uri::from_str(&websocket_url)?;
        tracing::debug!("Connecting to: {}", endpoint_uri);

        let in_flight_requests =
            InFlightRequests(Arc::new((AtomicUsize::new(0), Mutex::new(HashMap::new()))));

        let (websocket_conn, _resp) = tokio_tungstenite::connect_async(endpoint_uri).await?;
        let (ws_sink, mut ws_stream) = websocket_conn.split();
        let ws_sink = Arc::new(Mutex::new(ws_sink));
        let cancellation_token = CancellationToken::new();

        let Some(initial_message) = ws_stream.next().await else {
            anyhow::bail!("Websocket closed upon arrival")
        };
        tracing::info!("Received initial message");

        let ServerToClientMessage::Init(config) = initial_message?.try_into()? else {
            anyhow::bail!("Expected first message to be client initialization")
        };
        tracing::info!(
            "Consumer client connection (id={}) established with Opsqueue server {}",
            config.conn_id,
            config.version_info,
        );

        if config.version_info != crate::version_info() {
            tracing::warn!(
                "Careful! Consumer and Server use different Opsqueue library versions! Client is version {} whereas Server is version {}.",
                crate::version_info(),
                config.version_info,
            );
        }

        let healthy = Arc::new(AtomicBool::new(true));
        tokio::spawn(Self::background_task(
            cancellation_token.clone(),
            healthy.clone(),
            in_flight_requests.clone(),
            ws_stream,
            ws_sink.clone(),
            config,
        ));

        let me = Self {
            in_flight_requests,
            _bg_handle: cancellation_token.drop_guard(),
            healthy,
            ws_sink,
        };
        Ok(me)
    }

    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn background_task(
        cancellation_token: CancellationToken,
        healthy: Arc<AtomicBool>,
        in_flight_requests: InFlightRequests,
        mut ws_stream: impl Stream<Item = tungstenite::Result<tungstenite::Message>> + Unpin,
        ws_sink: Arc<Mutex<SplitSink<WebsocketTcpStream, Message>>>,
        config: ConsumerConfig,
    ) {
        let mut heartbeat_interval = tokio::time::interval(config.heartbeat_interval);
        let mut heartbeats_missed = 0;
        loop {
            yield_now().await;
            select! {
                () = cancellation_token.cancelled() => break,
                _ = heartbeat_interval.tick() => {
                    if heartbeats_missed > config.max_missable_heartbeats {
                        tracing::warn!("We missed too many heartbeats! Closing connection and marking client as unhealthy.");
                        // Mark ourselves as unhealthy:
                        healthy.store(false, std::sync::atomic::Ordering::Relaxed);
                        // For good measure, let's close the WebSocket connection early:
                        let _ = ws_sink.lock().await.close().await;
                        // And now exit the background task, which means all remaining in-flight requests immediately fail as well
                        break
                    }
                    // NOTE: We don't need to send a heartbeat as client; only the server needs to.
                    // We only need to track missed heartbeats.
                    heartbeats_missed += 1;
                },
                msg = ws_stream.next() => {
                    heartbeat_interval.reset();
                    heartbeats_missed = 0;
                    match msg {
                        None => {
                            tracing::debug!("Opsqueue consumer client background task closing as WebSocket connection closed");
                            break;
                        }
                        Some(Err(e)) => {
                            tracing::error!("Opsqueue consumer client background task closing, reason: {e}");
                            break;
                        },
                        Some(Ok(msg)) => {
                            if msg.is_close() {
                                tracing::debug!("Opsqueue consumer client background task closing as WebSocket connection closed");
                                break
                            } else if msg.is_ping() {
                                tracing::debug!("Received Heartbeat, expecting auto-pong");
                            } else if msg.is_pong() {
                                tracing::debug!("Received Pong reply to heartbeat, nice!");
                            } else if msg.is_binary() {
                                let msg: ServerToClientMessage = msg.try_into().expect("Unparsable ServerToClientMessage");
                                match msg {
                                    ServerToClientMessage::Sync(envelope) => {
                                        let Err(contents) = in_flight_requests.send(envelope).await else { continue; };
                                        tracing::warn!("Failed to send envelope to in-flight requests");
                                        match contents {
                                            SyncServerToClientResponse::ChunksReserved(reservation) => {
                                                let Ok(chunks) = reservation else { continue; };
                                                for chunk in &chunks {
                                                    let chunk_id = ChunkId::from((chunk.0.submission_id, chunk.0.chunk_index));
                                                    // Send message to the server to indicate that we are no longer
                                                    // reserving this chunk, so that it can be re-reserved by other
                                                    // consumers.
                                                    let Err(internal_error): Result<_, InternalConsumerClientError> = Self::async_request(
                                                        &in_flight_requests,
                                                        &ws_sink,
                                                        ClientToServerMessage::FailChunk {
                                                            id: chunk_id,
                                                            failure: "Requester disappeared".into(),
                                                        },
                                                    ).await else { continue; };
                                                    tracing::error!(error = &internal_error as &dyn Error, chunk_id = ?chunk_id, "Failed to return unhandled reservation to server");
                                                }
                                            },
                                        }
                                    },
                                    ServerToClientMessage::Async(msg) => {
                                        match msg {
                                            AsyncServerToClientMessage::ChunkReservationExpired(chunk_id) => {
                                                tracing::info!("Server indicated that we took too long with {chunk_id:?}, and now our reservation has expired.");
                                                // Send message to the server to indicate that we are no longer
                                                // reserving this chunk, so that it can be re-reserved by other
                                                // consumers. This round-trip is necessary to avoid a race condition
                                                // where we sent the success or failure of the chunk to the server,
                                                // while we lost the reservation window.
                                                //
                                                // In a race where a chunk success was already sent to the server, this
                                                // change allows that chunk to succeed and not be spuriously retried.
                                                // Connection ordering guarantees the round-trip of the reservation
                                                // expiry will come after the success.
                                                let Err(internal_error): Result<_, InternalConsumerClientError> = Self::async_request(
                                                    &in_flight_requests,
                                                    &ws_sink,
                                                    ClientToServerMessage::FailChunk {
                                                        id: chunk_id,
                                                        failure: "Reservation expired".into(),
                                                    },
                                                ).await else { continue; };
                                                tracing::error!(error = &internal_error as &dyn Error, chunk_id = ?chunk_id, "Failed to return expired reservation to server");
                                            },
                                        }
                                    }
                                    ServerToClientMessage::Init(_) => tracing::error!("Initialization message received after client loop start! Ignoring.")
                                }
                            }
                        },
                    }
                }
            }
        }
        // Clear any and all in-flight requests on exit of the background task.
        // This ensures that any waiting requests immediately return with an error as well.
        in_flight_requests.clear().await;
    }

    async fn sync_request(
        &self,
        request: ClientToServerMessage,
    ) -> Result<SyncServerToClientResponse, InternalConsumerClientError> {
        let (nonce, oneshot_receiver) = self.in_flight_requests.next_nonce_with_oneshot().await;
        let envelope = Envelope {
            nonce,
            contents: request,
        };
        let () = self.ws_sink.lock().await.send(envelope.into()).await?;
        let resp = oneshot_receiver.await?;
        Ok(resp)
    }

    async fn async_request(
        in_flight_requests: &InFlightRequests,
        ws_sink: &Mutex<SplitSink<WebsocketTcpStream, Message>>,
        request: ClientToServerMessage,
    ) -> Result<(), InternalConsumerClientError> {
        let nonce = in_flight_requests.next_nonce();
        let envelope = Envelope {
            nonce,
            contents: request,
        };
        let () = ws_sink.lock().await.send(envelope.into()).await?;
        Ok(())
    }

    /// Request chunks to reserve.
    ///
    /// # Errors
    ///
    /// Returns an error if the server rejects the request or if websocket/serialization
    /// communication fails.
    pub async fn reserve_chunks(
        &self,
        max: usize,
        strategy: Strategy,
    ) -> Result<Vec<(Chunk, Submission)>, E<InternalConsumerClientError, IncorrectUsage<LimitIsZero>>>
    {
        let SyncServerToClientResponse::ChunksReserved(resp) = self
            .sync_request(ClientToServerMessage::WantToReserveChunks { max, strategy })
            .await?;
        let chunks = resp.map_err(E::R)?;
        Ok(chunks)
    }

    /// Report chunk completion to the server.
    ///
    /// # Errors
    ///
    /// Returns an error if websocket communication fails.
    pub async fn complete_chunk(
        &self,
        id: ChunkId,
        output_content: chunk::Content,
    ) -> Result<(), InternalConsumerClientError> {
        Self::async_request(
            &self.in_flight_requests,
            &self.ws_sink,
            ClientToServerMessage::CompleteChunk { id, output_content },
        )
        .await
    }

    /// Report chunk failure to the server.
    ///
    /// # Errors
    ///
    /// Returns an error if websocket communication fails.
    pub async fn fail_chunk(
        &self,
        id: ChunkId,
        failure: String,
    ) -> Result<(), InternalConsumerClientError> {
        Self::async_request(
            &self.in_flight_requests,
            &self.ws_sink,
            ClientToServerMessage::FailChunk { id, failure },
        )
        .await
    }
}

#[derive(thiserror::Error, Debug)]
pub enum InternalConsumerClientError {
    #[error("Low-level error in the websocket connection: {0}")]
    LowLevelWebsocketError(#[from] tokio_tungstenite::tungstenite::Error),
    #[error(
        "The oneshot channel to receive a sync response to an earlier request was dropped before a response was received: {0}"
    )]
    OneshotSenderDropped(#[from] RecvError),
    #[error("Expected the sync response of kind {expected} but received {actual:?}")]
    UnexpectedSyncResponse {
        actual: SyncServerToClientResponse,
        expected: Box<str>,
    },
}

impl<R> From<InternalConsumerClientError> for E<InternalConsumerClientError, R> {
    fn from(value: InternalConsumerClientError) -> Self {
        E::L(value)
    }
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
mod tests {
    use std::time::Duration;

    use chunk::ChunkSize;
    use tokio::task::yield_now;
    use tokio_util::task::TaskTracker;

    use crate::{common::StrategicMetadataMap, db};

    use super::*;

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    pub async fn test_fetch_chunks(pool: sqlx::SqlitePool) {
        let db_pools = db::DBPools::from_test_pool(&pool);
        let uri = "0.0.0.0:10083";
        let ws_uri = "ws://0.0.0.0:10083";
        let cancellation_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        let mut conn = db_pools.writer_conn().await.unwrap();
        let input_chunks = vec![
            Some("a".into()),
            Some("b".into()),
            Some("c".into()),
            Some("d".into()),
            Some("e".into()),
        ];
        crate::common::submission::db::insert_submission_from_chunks(
            None,
            input_chunks.clone(),
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            false,
            &mut conn,
        )
        .await
        .unwrap();

        let _server_handle = task_tracker.spawn(crate::consumer::server::serve_for_tests(
            db_pools,
            uri.into(),
            cancellation_token,
            Duration::from_mins(1),
        ));

        yield_now().await;

        let client = Client::new(ws_uri).await.unwrap();
        yield_now().await;

        let chunks = client
            .reserve_chunks(3, Strategy::Oldest)
            .await
            .expect("No internal error");
        yield_now().await;

        assert_eq!(
            chunks
                .iter()
                .map(|(c, _s)| c.input_content.clone())
                .collect::<Vec<Option<Vec<u8>>>>(),
            input_chunks[0..3]
        );

        // NOTE: We ensure to fetch exactly the amount left;
        // if we fetch more, the server will only respond when new chunks are inserted,
        // which would make this test hang
        let two = client.reserve_chunks(1, Strategy::Oldest);
        let three = client.reserve_chunks(1, Strategy::Oldest);

        yield_now().await;

        let _three = three.await;
        let _two = two.await;
    }
}

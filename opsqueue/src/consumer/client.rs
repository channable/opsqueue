use std::{
    collections::HashMap,
    str::FromStr,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use arc_swap::ArcSwapOption;
use futures::{stream::SplitSink, SinkExt, Stream, StreamExt};
use http::Uri;
use tokio::{net::TcpStream, sync::oneshot::error::RecvError};
use tokio::{
    select,
    sync::{oneshot, Mutex},
    task::yield_now,
};
use tokio_tungstenite::{
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::{CancellationToken, DropGuard};
// use tokio_websockets::{MaybeTlsStream, Message, WebSocketStream};

use crate::{
    common::{
        chunk::{self, Chunk, ChunkId},
        errors::{IncorrectUsage, LimitIsZero, E},
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

type InFlightRequests = Arc<
    Mutex<(
        usize,
        HashMap<usize, oneshot::Sender<SyncServerToClientResponse>>,
    )>,
>;
type WebsocketTcpStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A wrapper around the actual client,
/// ensuring that the client:
/// - Is initialized lazily
/// - Is reset on low-level failures
/// - And therefore, that it is resilient to temporary network failures
#[derive(Debug)]
pub struct OuterClient(ArcSwapOption<Client>, Box<str>);

impl OuterClient {
    pub fn new(url: &str) -> Self {
        Self(None.into(), url.into())
    }

    pub fn address(&self) -> &str {
        &self.1
    }

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
        let inner = self.0.load();
        if inner.is_none() || inner.as_ref().is_some_and(|c| !c.is_healthy()) {
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
}

// TOOD: Set max retries to `None`;
// will require either writing our own Backoff (iterator)
// or extending the backon crate.
fn retry_policy() -> impl BackoffBuilder {
    FibonacciBuilder::default()
        .with_jitter()
        .with_min_delay(Duration::from_millis(10))
        .with_max_delay(Duration::from_secs(5))
        .without_max_times()
}

#[derive(Debug)]
pub struct Client {
    in_flight_requests: InFlightRequests,
    ws_sink: Arc<Mutex<SplitSink<WebsocketTcpStream, Message>>>,
    healthy: Arc<AtomicBool>,
    _bg_handle: DropGuard,
}

impl Client {
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        // Ensure that the given URL is always a websocket URL; tungstenite requires this
        let endpoint_url = if url.starts_with("ws://") || url.starts_with("wss://") {
            format!("{url}/consumer")
        } else {
            format!("ws://{url}/consumer")
        };
        let endpoint_uri = Uri::from_str(&endpoint_url)?;
        tracing::debug!("Connecting to: {}", endpoint_uri);

        let in_flight_requests: InFlightRequests = Arc::new(Mutex::new((0, HashMap::new())));

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
            )
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
                _ = cancellation_token.cancelled() => break,
                _ = heartbeat_interval.tick() => {
                    if heartbeats_missed > config.max_missable_heartbeats {
                        tracing::warn!("We missed too many heartbeats! Closing connection and marking client as unhealthy.");
                        // Mark ourselves as unhealthy:
                        healthy.store(false, std::sync::atomic::Ordering::Relaxed);
                        // For good measure, let's close the WebSocket connection early:
                        let _ = ws_sink.lock().await.close().await;
                        // And now exit the background task, which means all remaining in-flight requests immediately fail as well
                        break
                    } else {
                        // NOTE: We don't need to send a heartbeat as client; only the server needs to.
                        // We only need to track missed heartbeats.
                        heartbeats_missed += 1;
                    }
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
                                tracing::debug!("Received Heartbeat, expect auto-pong");
                                // let _ = ws_sink.lock().await.send(Message::pong("heartbeat")).await;
                            } else if msg.is_pong() {
                                tracing::debug!("Received Pong reply to heartbeat, nice!");
                            } else if msg.is_binary() {
                                let msg: ServerToClientMessage = msg.try_into().expect("Unparseable ServerToClientMessage");
                                match msg {
                                    ServerToClientMessage::Sync(envelope) => {
                                        let mut in_flight_requests = in_flight_requests.lock().await;
                                        // Handle the response to some earlier request
                                        let oneshot_receiver = in_flight_requests.1.remove(&envelope.nonce).expect("Received response with nonce that matches none of the open requests");
                                        let _ = oneshot_receiver.send(envelope.contents);

                                    },
                                    ServerToClientMessage::Async(msg) => {
                                        // Handle a message from the server that was not associated with an earlier request
                                        match msg {
                                            AsyncServerToClientMessage::ChunkReservationExpired(_chunk_id) => {
                                                tracing::error!("Client could cancel execution of current work, but this is not implemented yet.");
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
        let mut in_flight_requests = in_flight_requests.lock().await;
        in_flight_requests.1.clear();
        in_flight_requests.0 = 0;
    }

    async fn sync_request(
        &self,
        request: ClientToServerMessage,
    ) -> Result<SyncServerToClientResponse, InternalConsumerClientError> {
        let (oneshot_sender, oneshot_receiver) = oneshot::channel();
        {
            let mut in_flight_requests = self.in_flight_requests.lock().await;
            let nonce = in_flight_requests.0;
            in_flight_requests.0 = in_flight_requests.0.wrapping_add(1);
            let envelope = Envelope {
                nonce,
                contents: request,
            };
            in_flight_requests.1.insert(nonce, oneshot_sender);
            let () = self.ws_sink.lock().await.send(envelope.into()).await?;
        }
        let resp = oneshot_receiver.await?;
        Ok(resp)
    }

    async fn async_request(
        &self,
        request: ClientToServerMessage,
    ) -> Result<(), InternalConsumerClientError> {
        let mut in_flight_requests = self.in_flight_requests.lock().await;
        let nonce = in_flight_requests.0;
        in_flight_requests.0 = in_flight_requests.0.wrapping_add(1);
        let envelope = Envelope {
            nonce,
            contents: request,
        };
        let () = self.ws_sink.lock().await.send(envelope.into()).await?;
        Ok(())
    }

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

    pub async fn complete_chunk(
        &self,
        id: ChunkId,
        output_content: chunk::Content,
    ) -> Result<(), InternalConsumerClientError> {
        self.async_request(ClientToServerMessage::CompleteChunk { id, output_content })
            .await
    }

    pub async fn fail_chunk(
        &self,
        id: ChunkId,
        failure: String,
    ) -> Result<(), InternalConsumerClientError> {
        self.async_request(ClientToServerMessage::FailChunk { id, failure })
            .await
    }
}

#[derive(thiserror::Error, Debug)]
pub enum InternalConsumerClientError {
    #[error("Low-level error in the websocket connection: {0}")]
    LowLevelWebsocketError(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("The oneshot channel to receive a sync response to an earlier request was dropped before a response was received: {0}")]
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

    #[sqlx::test]
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
            &mut conn,
        )
        .await
        .unwrap();

        let _server_handle = task_tracker.spawn(crate::consumer::server::serve_for_tests(
            db_pools,
            uri.into(),
            cancellation_token,
            Duration::from_secs(60),
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

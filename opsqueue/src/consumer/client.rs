use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use arc_swap::ArcSwapOption;
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use http::Uri;
use retry_if::ExponentialBackoffConfig;
use tokio::net::TcpStream;
use tokio::{
    select,
    sync::{oneshot, Mutex},
    task::yield_now,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tokio_websockets::{MaybeTlsStream, Message, WebSocketStream};

use crate::{
    common::chunk::{self, Chunk, ChunkId},
    consumer::common::{AsyncServerToClientMessage, Envelope},
};

use super::{
    common::{ClientToServerMessage, ServerToClientMessage, SyncServerToClientResponse},
    strategy::Strategy,
};

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
    pub async fn reserve_chunks(&self, max: usize, strategy: Strategy) -> Result<Vec<Chunk>, anyhow::Error> {
        self.ensure_initialized().await;
        let res = self.0.load().as_ref().expect("Should always be initialized after `.ensure_initialized()").reserve_chunks(max, strategy).await;
        if res.is_err() { // TODO: Only throw away inner client on connection failure style errors
            self.0.store(None);
        }
        res
    }

    pub async fn complete_chunk(
        &self,
        id: ChunkId,
        output_content: chunk::Content,
    ) -> anyhow::Result<()> {
        self.ensure_initialized().await;
        let res = self.0.load().as_ref().expect("Should always be initialized after `.ensure_initialized()").complete_chunk(id, output_content).await;
        if res.is_err() { // TODO: Only throw away inner client on connection failure style errors
            self.0.store(None);
        }
        res
    }

    async fn ensure_initialized(&self) {
        if self.0.load().is_none() {
            let client = loop {
                match self.initialize().await {
                    Ok(client) => break client,
                    Err(_) => {
                        // NOTE: This is extremely unlikely to occur; it means that the Opsqueue couldn't be reached
                        // for a duration close to u32::MAX * 10sec.
                        // TODO: Better would be to fix the retry_if crate to support indefinite retries!
                        continue;
                    }
                }
            };
            log::info!("Consumer client connection established");
            self.0.store(Some(Arc::new(client)));
        }
    }

    #[retry_if::retry(BACKOFF_CONFIG, retry_errs)]
    async fn initialize(&self) -> anyhow::Result<Client> {
        Client::new(&self.1).await
    }
}

const BACKOFF_CONFIG: ExponentialBackoffConfig = ExponentialBackoffConfig {
    max_retries: i32::MAX, // TODO: This is brittle. Maybe improve the `retry_if` crate to support indefinite retries?
    t_wait: Duration::from_millis(50),
    backoff: 2.0,
    t_wait_max: None,
    backoff_max: Some(Duration::from_secs(10)),
};

fn retry_errs<T>(result: &anyhow::Result<T>) -> bool {
    match result {
        Err(err) => {
            log::debug!("Error establishing consumer client WS connection. (Will retry). Details: {err:?}");
            true
        },
        Ok(_) => false,
    }
}

#[derive(Debug)]
pub struct Client {
    in_flight_requests: InFlightRequests,
    ws_sink: Mutex<SplitSink<WebsocketTcpStream, Message>>,
    _bg_handle: DropGuard,
}

impl Client {
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let uri = Uri::from_str(url)?;
        let in_flight_requests: InFlightRequests = Arc::new(Mutex::new((0, HashMap::new())));

        let (websocket_conn, _resp) = tokio_websockets::ClientBuilder::from_uri(uri)
            .connect()
            .await?;
        let (ws_sink, ws_stream) = websocket_conn.split();
        let cancellation_token = CancellationToken::new();

        tokio::spawn(Self::background_task(
            cancellation_token.clone(),
            in_flight_requests.clone(),
            ws_stream,
        ));

        let me = Self {
            in_flight_requests,
            _bg_handle: cancellation_token.drop_guard(),
            ws_sink: Mutex::new(ws_sink),
        };
        Ok(me)
    }

    async fn background_task(
        cancellation_token: CancellationToken,
        in_flight_requests: InFlightRequests,
        mut ws_stream: SplitStream<WebsocketTcpStream>,
    ) {
        loop {
            yield_now().await;
            select! {
                _ = cancellation_token.cancelled() => break,
                msg = ws_stream.next() => {
                    match msg {
                        None => {
                            log::debug!("Opsqueue consumer client background task closing as WebSocket connection closed");
                            break;
                        }
                        Some(Err(e)) => {
                            log::error!("Opsqueue consumer client background task closing, reason: {e}");
                            break;
                        },
                        Some(Ok(msg)) => {
                            if msg.is_close() {
                                log::debug!("Opsqueue consumer client background task closing as WebSocket connection closed");
                                break
                            } else if msg.is_ping() {
                                log::warn!("Received Heartbeat. (TODO: Handle)");
                            } else {
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
                                                log::warn!("TODO: Client should cancel execution of current work if possible");
                                            },
                                        }
                                    }
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

    }

    async fn request(
        &self,
        request: ClientToServerMessage,
    ) -> anyhow::Result<SyncServerToClientResponse> {
        let (oneshot_sender, oneshot_receiver) = oneshot::channel();
        {
            let mut in_flight_requests = self.in_flight_requests.lock().await;
            let nonce = in_flight_requests.0.wrapping_add(1);
            let envelope = Envelope {
                nonce: nonce,
                contents: request,
            };
            in_flight_requests.1.insert(nonce, oneshot_sender);
            let () = self
                .ws_sink
                .lock()
                .await
                .send(envelope.into())
                .await?;
        }
        let resp = oneshot_receiver.await?;
        Ok(resp)
    }

    pub async fn reserve_chunks(
        &self,
        max: usize,
        strategy: Strategy,
    ) -> anyhow::Result<Vec<Chunk>> {
        let resp = self
            .request(ClientToServerMessage::WantToReserveChunks { max, strategy })
            .await?;
        match resp {
            SyncServerToClientResponse::ChunksReserved(chunks) => Ok(chunks),
            _ => anyhow::bail!("Unexpected response from server: {:?}", resp),
        }
    }

    pub async fn complete_chunk(
        &self,
        id: ChunkId,
        output_content: chunk::Content,
    ) -> anyhow::Result<()> {
        let resp = self
            .request(ClientToServerMessage::CompleteChunk { id, output_content })
            .await?;
        match resp {
            SyncServerToClientResponse::ChunkCompleted => Ok(()),
            _ => anyhow::bail!("Unexpected response from server: {:?}", resp),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::task::yield_now;
    use tokio_util::task::TaskTracker;

    use crate::consumer::server::ConsumerServerState;

    use super::*;

    #[sqlx::test]
    pub async fn test_fetch_chunks(pool: sqlx::SqlitePool) {
        let uri = "0.0.0.0:10083";
        let ws_uri = "ws://0.0.0.0:10083";
        let cancellation_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        let mut conn = pool.acquire().await.unwrap();
        let input_chunks = vec![
            Some("a".into()),
            Some("b".into()),
            Some("c".into()),
            Some("d".into()),
            Some("e".into()),
        ];
        crate::common::submission::insert_submission_from_chunks(
            None,
            input_chunks.clone(),
            &mut conn,
        )
        .await
        .unwrap();

        let _server_handle = task_tracker.spawn(
            ConsumerServerState::new(pool.clone(), Duration::from_secs(60), uri)
                .await
                .run(cancellation_token, task_tracker.clone()),
        );

        yield_now().await;

        let client = Client::new(ws_uri).await.unwrap();
        yield_now().await;

        let chunks = client.reserve_chunks(3, Strategy::Oldest).await.unwrap();
        yield_now().await;

        assert_eq!(
            chunks
                .iter()
                .map(|c| c.input_content.clone())
                .collect::<Vec<Option<Vec<u8>>>>(),
            input_chunks[0..3]
        );

        let two = client.reserve_chunks(3, Strategy::Oldest);
        let three = client.reserve_chunks(3, Strategy::Oldest);

        yield_now().await;

        let three = three.await;
        let two = two.await;
    }
}

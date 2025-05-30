use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::extract::ws::{Message, WebSocket};
use tokio::{
    select,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        Notify,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        chunk::ChunkId,
        errors::{DatabaseError, E},
    },
    consumer::{
        common::{
            AsyncServerToClientMessage, ClientToServerMessage, ConsumerConfig, Envelope,
            ServerToClientMessage, SyncServerToClientResponse,
        },
        strategy::Strategy,
    },
};

use super::{state::ConsumerState, ServerState};

#[derive(Debug, Clone)]
pub struct RetryReservation {
    nonce: usize,
    max: usize,
    strategy: Strategy,
}

pub struct ConsumerConn {
    id: ConsumerConnectionId,
    consumer_state: ConsumerState,
    cancellation_token: CancellationToken,
    ws_stream: WebSocket,
    notify_on_insert: Arc<Notify>,
    heartbeat_interval: tokio::time::Interval,
    heartbeats_missed: usize,
    max_missable_heartbeats: usize,
    // Channel with which the rest of the server can send messages to this particular client
    tx: UnboundedSender<ChunkId>,
    rx: UnboundedReceiver<ChunkId>,

    tx2: UnboundedSender<RetryReservation>,
    rx2: UnboundedReceiver<RetryReservation>,
}

impl ConsumerConn {
    pub fn new(server_state: &Arc<ServerState>, ws_stream: WebSocket) -> Self {
        let heartbeat_interval =
            tokio::time::interval(server_state.config.heartbeat_interval.into());
        let heartbeats_missed = 0;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel();
        let consumer_state = ConsumerState::new(server_state);
        let cancellation_token = server_state.cancellation_token.clone();
        let notify_on_insert = server_state.notify_on_insert.clone();

        // Environment variable determines whether to assign color names for new connection IDs.
        // If set, colors are used for the first 24 connections.
        let id = if std::env::var("OPSQUEUE_DEBUG_CONNECTIONS").is_ok() {
            ConsumerConnectionId::color()
        } else {
            ConsumerConnectionId::uuid()
        };

        tracing::debug!("Creating consumer connection with id {id}");

        Self {
            id,
            consumer_state,
            ws_stream,
            notify_on_insert,
            heartbeat_interval,
            heartbeats_missed,
            max_missable_heartbeats: server_state.config.max_missable_heartbeats,
            tx,
            rx,
            tx2,
            rx2,
            cancellation_token,
        }
    }

    /// Runs the consumer websocket connection loop
    /// Blocks until the loop is stopped (because the connection is closed intentionally or by network failure).
    pub async fn run(mut self) -> Result<(), ConsumerConnError> {
        self.initialize().await?;
        loop {
            select! {
                // On shutdown, gracefully shut down and ignore any errors
                () = self.cancellation_token.cancelled() => return {
                    self.graceful_shutdown().await;
                    Ok(())
                },
                // When the heartbeat interval elapsed, send the next one
                _ = self.heartbeat_interval.tick() => self.beat_heart().await?,
                // When a normal message is received, handle it
                msg = self.ws_stream.recv() => {
                    match msg {
                        // Socket closed (stream closed before receiving WS 'close' message, ungraceful shutdown)
                        None => return Ok(()),
                        // Socket received a close message (graceful WS shutdown)
                        Some(Ok(Message::Close(_))) => return Ok(()),
                        // Socket had a problem (protocol violation, sending too much data, closed, etc.)
                        Some(Err(err)) => return Err(ConsumerConnError::LowLevelWebsocketError(err)),
                        // Socket received a normal message
                        Some(Ok(msg)) => self.handle_incoming_msg(msg).await?,
                    }
                },
                // When we are notified that we can retry an earlier failing reservation:
                Some(RetryReservation{nonce, max, strategy}) = self.rx2.recv() => {
                    tracing::debug!("Retrying reservation for nonce {nonce} / {max} / {strategy:?}");
                    self.handle_incoming_client_message(Envelope {nonce, contents: ClientToServerMessage::WantToReserveChunks { max, strategy }}).await?
                }
                // When a message from elsewhere in the app is received, pass it forward
                // (Currently there is only one kind of these, namely a chunk reservation having expired)
                Some(chunk_id) = self.rx.recv() => {
                    let msg = ServerToClientMessage::Async(AsyncServerToClientMessage::ChunkReservationExpired(chunk_id));
                    self.ws_stream.send(msg.into()).await?;
                },
            }
        }
    }

    async fn graceful_shutdown(self) {
        const GRACEFUL_WEBSOCKET_CLOSE_TIMEOUT: Duration = Duration::from_millis(100);
        select! {
            _ = self.ws_stream.close() => {},
            // This branch is taken if things were taking too long:
            () = tokio::time::sleep(GRACEFUL_WEBSOCKET_CLOSE_TIMEOUT) => {},
        }
    }

    #[tracing::instrument(fields(conn = %self.id), skip(self))]
    async fn initialize(&mut self) -> Result<(), ConsumerConnError> {
        tracing::debug!("Sending initialization message");
        // Send the initialization message. The consumer client is waiting for this one before it enters its main loop.
        let init_msg = ServerToClientMessage::Init(ConsumerConfig {
            max_missable_heartbeats: self.max_missable_heartbeats,
            heartbeat_interval: self.heartbeat_interval.period(),
            version_info: crate::version_info(),
            conn_id: self.id.to_string(),
        })
        .into();
        self.ws_stream.send(init_msg).await?;
        tracing::info!("Initialization message sent");
        Ok(())
    }

    // Manages heartbeating:
    // Sends the next heartbeat, or exits with an error if too many were already missed.
    #[tracing::instrument(fields(conn = %self.id), skip(self))]
    async fn beat_heart(&mut self) -> Result<(), ConsumerConnError> {
        tracing::debug!("Sending heartbeat");
        if self.heartbeats_missed > self.max_missable_heartbeats {
            tracing::warn!("Too many heartbeat misses, closing connection.");
            Err(ConsumerConnError::HeartbeatFailure)
        } else {
            tracing::debug!("Pinging");
            let _ = self.ws_stream.send(Message::Ping("💓".into())).await;
            self.heartbeats_missed += 1;
            Ok(())
        }
    }

    #[tracing::instrument(fields(conn = %self.id), skip(self, msg))]
    async fn handle_incoming_msg(&mut self, msg: Message) -> Result<(), ConsumerConnError> {
        // Any message means the other side is still alive:
        self.heartbeat_interval.reset();
        self.heartbeats_missed = 0;

        match msg {
            // NOTE: Not manually sending 'pong', this is done automatically.
            Message::Ping(_) => tracing::trace!("Received heartbeat"),
            Message::Pong(_) => tracing::trace!("Received pong reply to earlier heartbeat, nice."),
            Message::Binary(_) => {
                self.handle_incoming_client_message(msg.try_into()?).await?;
            }
            _ => {
                return Err(ConsumerConnError::UnexpectedWSMessageType(
                    anyhow::format_err!("Unexpected message format  {:?}", msg),
                ))
            }
        }

        Ok(())
    }

    async fn handle_incoming_client_message(
        &mut self,
        msg: Envelope<ClientToServerMessage>,
    ) -> Result<(), ConsumerConnError> {
        use ClientToServerMessage::*;
        use SyncServerToClientResponse::*;
        let maybe_response = match msg.contents {
            WantToReserveChunks { max, strategy } => {
                let chunks_or_err = self
                    .consumer_state
                    .fetch_and_reserve_chunks(strategy.clone(), max, &self.tx)
                    .await;
                match chunks_or_err {
                    Err(e) => {
                        tracing::error!("Failed to reserve chunks: {}", e);
                        match e {
                            // In the unlikely event of a DatabaseError,
                            // this is not the client's fault but _our_ fault.
                            // Therefore, we send the client an empty vec back, and log the error.
                            E::L(DatabaseError(_)) => Some(ChunksReserved(Ok(Vec::new()))),
                            E::R(incorrect_usage) => Some(ChunksReserved(Err(incorrect_usage))),
                        }
                    }
                    Ok(vals) if !vals.is_empty() => Some(ChunksReserved(Ok(vals))),
                    Ok(_) => {
                        // No work to do right now. Retry when new work is inserted.
                        tracing::debug!(
                            "No work to do for {} / {max} / {strategy:?}, retrying later",
                            msg.nonce
                        );
                        let notifier = self.notify_on_insert.clone();
                        let tx2 = self.tx2.clone();
                        tokio::spawn(async move {
                            notifier.notified().await;
                            let _ = tx2.send(RetryReservation {
                                nonce: msg.nonce,
                                max,
                                strategy,
                            });
                        });
                        None
                    }
                }
            }
            CompleteChunk { id, output_content } => {
                self.consumer_state.complete_chunk(id, output_content).await;
                // .map_err(anyhow::Error::from)?;
                None
            }
            FailChunk { id, failure } => {
                self.consumer_state.fail_chunk(id, failure).await;
                None
            }
        };

        if let Some(response) = maybe_response {
            let enveloped_response = Envelope {
                nonce: msg.nonce,
                contents: response,
            };
            self.ws_stream
                .send(ServerToClientMessage::Sync(enveloped_response).into())
                .await?
        }

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Error while handling a consumer connection")]
pub enum ConsumerConnError {
    // Internal (opsqueue server is doing something wrong):
    DbError(#[from] DatabaseError),
    UnparsableServerToClientMessage(ciborium::ser::Error<std::io::Error>),
    Other(#[from] anyhow::Error),
    // External (caused by client misbehaving or wrong version or network failure):
    HeartbeatFailure,
    UnreadableClientToServerMessage(ciborium::de::Error<std::io::Error>),
    UnexpectedWSMessageType(anyhow::Error),
    LowLevelWebsocketError(axum::Error),
}

impl ConsumerConnError {
    pub fn is_internal_error(&self) -> bool {
        matches!(
            self,
            ConsumerConnError::DbError(_)
                | ConsumerConnError::UnparsableServerToClientMessage(_)
                | ConsumerConnError::Other(_)
        )
    }
}

impl From<sqlx::Error> for ConsumerConnError {
    fn from(err: sqlx::Error) -> Self {
        ConsumerConnError::DbError(err.into())
    }
}

impl From<axum::Error> for ConsumerConnError {
    fn from(err: axum::Error) -> Self {
        ConsumerConnError::LowLevelWebsocketError(err)
    }
}

impl From<ciborium::de::Error<std::io::Error>> for ConsumerConnError {
    fn from(err: ciborium::de::Error<std::io::Error>) -> Self {
        ConsumerConnError::UnreadableClientToServerMessage(err)
    }
}
impl From<ciborium::ser::Error<std::io::Error>> for ConsumerConnError {
    fn from(err: ciborium::ser::Error<std::io::Error>) -> Self {
        ConsumerConnError::UnparsableServerToClientMessage(err)
    }
}

/// A unique ID for a consumer connection.
#[derive(Debug, Clone)]
pub struct ConsumerConnectionId(String);

impl ConsumerConnectionId {
    /// Generate the consumer id from a UUID.
    pub fn uuid() -> ConsumerConnectionId {
        ConsumerConnectionId(uuid::Uuid::now_v7().to_string())
    }
    /// Use a color for an ID. This is more readable for debugging, but doesn't exactly
    /// scale. Uses a global list of colors that are dealt out in the same order every
    /// time. The colors list runs out after 24, and after that it'll fall back to UUIDs.
    pub fn color() -> ConsumerConnectionId {
        static mut INDEX: AtomicUsize = AtomicUsize::new(0);
        const COLORS: [&'static str; 24] = [
            "red",
            "green",
            "blue",
            "yellow",
            "purple",
            "orange",
            "cyan",
            "white",
            "black",
            "pink",
            "turquoise",
            "teal",
            "chartreuse",
            "brown",
            "amber",
            "violet",
            "magenta",
            "indigo",
            "lime",
            "gold",
            "aqua",
            "tangerine",
            "maroon",
            "fuchsia",
        ];

        // Safety: i think this is ok because we're doing it ✨ atomically ✨
        #[allow(static_mut_refs)]
        let i = unsafe { INDEX.fetch_add(1, Ordering::SeqCst) };

        COLORS
            .get(i)
            .map(|c| String::from(*c))
            .map(ConsumerConnectionId)
            .unwrap_or_else(|| Self::uuid())
    }
}

impl std::fmt::Display for ConsumerConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

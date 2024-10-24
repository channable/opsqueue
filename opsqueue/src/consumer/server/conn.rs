use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use tokio::{select, sync::mpsc::{UnboundedReceiver, UnboundedSender}};
use tokio_util::sync::CancellationToken;

use crate::{common::chunk::ChunkId, consumer::common::{AsyncServerToClientMessage, ClientToServerMessage, Envelope, ServerToClientMessage, SyncServerToClientResponse, HEARTBEAT_INTERVAL, MAX_MISSABLE_HEARTBEATS}};

use super::{state::ConsumerState, ServerState};


pub struct ConsumerConn {
    consumer_state: ConsumerState,
    cancellation_token: CancellationToken,
    ws_stream: WebSocket,
    heartbeat_interval: tokio::time::Interval,
    heartbeats_missed: usize,
    // Channel with which the rest of the server can send messages to this particular client
    tx: UnboundedSender<ChunkId>,
    rx: UnboundedReceiver<ChunkId>,
}

impl ConsumerConn {
    pub fn new(server_state: &ServerState, ws_stream: WebSocket) -> Self {
        let heartbeat_interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        let heartbeats_missed = 0;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let consumer_state = ConsumerState::new(server_state);
        let cancellation_token = server_state.cancellation_token.clone();

        Self {consumer_state, ws_stream, heartbeat_interval, heartbeats_missed, tx, rx, cancellation_token}
    }

    pub async fn run(mut self) -> Result<(), ConsumerConnError> {
        loop {
            select! {
                // On shutdown, gracefully shut down and ignore any errors
                () = self.cancellation_token.cancelled() => return Ok(self.graceful_shutdown().await),
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
                // When a message from elsewhere in the app is received, pass it forward
                // (Currently there is only one kind of these, namely a chunk reservation having expired)
                Some((submission_id, chunk_index)) = self.rx.recv() => {
                    let msg = ServerToClientMessage::Async(AsyncServerToClientMessage::ChunkReservationExpired((submission_id, chunk_index)));
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
        ()
    }


    // Manages heartbeating:
    // Sends the next heartbeat, or exits with an error if too many were already missed.
    async fn beat_heart(&mut self) -> Result<(), ConsumerConnError> {
        tracing::debug!("Sending heartbeat");
        if self.heartbeats_missed > MAX_MISSABLE_HEARTBEATS {
            tracing::warn!("Too many heartbeat misses, closing connection.");
            Err(ConsumerConnError::HeartbeatFailure)
        } else {
            let _ = self.ws_stream.send(Message::Ping("heartbeat".into())).await;
            self.heartbeats_missed += 1;
            Ok(())
        }
    }

    async fn handle_incoming_msg(&mut self, msg: Message) -> Result<(), ConsumerConnError> {
        // Any message means the other side is still alive:
        self.heartbeat_interval.reset();
        self.heartbeats_missed = 0;

        match msg {
            Message::Ping(_) => {
                tracing::trace!("Received heartbeat");
                // NOTE: Not manually sending 'pong', this is done automatically.
            },
            Message::Pong(_) => {
                tracing::trace!("Received pong reply to earlier heartbeat, nice.");
            }
            Message::Binary(_) => {
                self.handle_incoming_client_message(msg.try_into()?).await?;

            }
            _ => return Err(ConsumerConnError::UnexpectedWSMessageType(anyhow::format_err!("Unexpected message format  {:?}", msg))),
        }

        Ok(())
    }

    async fn handle_incoming_client_message(&mut self, msg: Envelope<ClientToServerMessage>) -> Result<(), ConsumerConnError> {
        use ClientToServerMessage::*;
        use SyncServerToClientResponse::*;
        let maybe_response = match msg.contents {
            WantToReserveChunks { max, strategy } => {
                let chunks = self.consumer_state.fetch_and_reserve_chunks(strategy, max, &self.tx).await?;
                Some(ChunksReserved(chunks))
            },
            CompleteChunk {id, output_content} => {
                self.consumer_state.complete_chunk(id, output_content).await?;
                Some(ChunkCompleted)
            },
            FailChunk {id, failure} => {
                self.consumer_state.fail_chunk(id, failure).await?;
                Some(ChunkFailed)
            },
        };

        if let Some(response) = maybe_response {
            let enveloped_response = Envelope {nonce: msg.nonce, contents: response };
            self.ws_stream.send(ServerToClientMessage::Sync(enveloped_response).into()).await?
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum ConsumerConnError {
    HeartbeatFailure,
    DbError(sqlx::Error),
    UnreadableClientToServerMessage(ciborium::de::Error<std::io::Error>),
    UnparsableServerToClientMessage(ciborium::ser::Error<std::io::Error>),
    UnexpectedWSMessageType(anyhow::Error),
    LowLevelWebsocketError(axum::Error),
}

impl From<sqlx::Error> for ConsumerConnError {
    fn from(err: sqlx::Error) -> Self {
        ConsumerConnError::DbError(err)
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

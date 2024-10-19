use std::collections::HashSet;
use std::time::Duration;

use futures::{SinkExt, StreamExt};

use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_websockets::{Message, WebSocketStream};

use crate::common::chunk::ChunkId;
use crate::consumer::common::{
    AsyncServerToClientMessage, ClientToServerMessage, Envelope, ServerToClientMessage,
    SyncServerToClientResponse,
};

#[derive(Debug)]
pub enum ClientConnError {
    HeartbeatFailure,
    DbError(sqlx::Error),
    UnreadableClientToServerMessage(ciborium::de::Error<std::io::Error>),
    UnparsableServerToClientMessage(ciborium::ser::Error<std::io::Error>),
    LowLevelWebsocketError(tokio_websockets::Error),
}

impl From<sqlx::Error> for ClientConnError {
    fn from(err: sqlx::Error) -> Self {
        ClientConnError::DbError(err)
    }
}

impl From<tokio_websockets::Error> for ClientConnError {
    fn from(err: tokio_websockets::Error) -> Self {
        ClientConnError::LowLevelWebsocketError(err)
    }
}

impl From<ciborium::de::Error<std::io::Error>> for ClientConnError {
    fn from(err: ciborium::de::Error<std::io::Error>) -> Self {
        ClientConnError::UnreadableClientToServerMessage(err)
    }
}
impl From<ciborium::ser::Error<std::io::Error>> for ClientConnError {
    fn from(err: ciborium::ser::Error<std::io::Error>) -> Self {
        ClientConnError::UnparsableServerToClientMessage(err)
    }
}

#[derive(Debug)]
pub struct ClientConn {
    // Websocket stream with which we communicate with the client
    ws_stream: WebSocketStream<TcpStream>,
    heartbeat_interval: tokio::time::Interval,
    heartbeats_missed: usize,
    // Channel with which the rest of the server can send messages to this particular client
    tx: UnboundedSender<ChunkId>,
    rx: UnboundedReceiver<ChunkId>,
    server_state: super::state::ConsumerServerState,
    reservations: HashSet<ChunkId>,
}

impl Drop for ClientConn {
    fn drop(&mut self) {
        for reservation in &self.reservations {
            self.server_state.finish_reservation(reservation);
        }
    }
}

impl ClientConn {
    pub fn new(
        server_state: super::state::ConsumerServerState,
        ws_stream: WebSocketStream<TcpStream>,
    ) -> Self {
        // TODO: make interval configurable
        let heartbeat_interval = tokio::time::interval(Duration::from_secs(10));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
            ws_stream,
            heartbeat_interval,
            heartbeats_missed: 0,
            tx,
            rx,
            server_state,
            reservations: HashSet::new(),
        }
    }

    pub async fn run(mut self) -> Result<(), ClientConnError> {
        loop {
            select! {
                msg = self.ws_stream.next() => {
                    match msg {
                        // Socket closed (normal connection close)
                        None => return Ok(()),
                        // Socket had a problem (protocol violation, sending too much data, closed, etc.)
                        Some(Err(err)) => return Err(ClientConnError::LowLevelWebsocketError(err)),
                        // Socket received a message
                        Some(Ok(msg)) => self.handle_incoming_msg(msg).await?,
                    }
                },
                Some((submission_id, chunk_index)) = self.rx.recv() => {
                    let msg = ServerToClientMessage::Async(AsyncServerToClientMessage::ChunkReservationExpired((submission_id, chunk_index)));
                    self.ws_stream.send(msg.into()).await?;
                },
                _ = self.heartbeat_interval.tick() => self.beat_heart().await?,
            }
        }
    }

    // Deals with any message arrived through the Websocket connection.
    async fn handle_incoming_msg(&mut self, msg: Message) -> Result<(), ClientConnError> {
        self.heartbeat_interval.reset();
        self.heartbeats_missed = 0;

        // Other side sent a heartbeat, send a heartbeat response
        if msg.is_ping() {
            self.ws_stream.send(Message::pong("heartbeat")).await?
        }

        // App-specific messages:
        // TODO extract deserializing to helper function on ClientToServerMessage
        if msg.is_binary() {
            self.handle_incoming_client_msg(msg.try_into()?).await?;
        }

        Ok(())
    }

    // Deals with app-specific messages arrived through the Websocket connection.
    async fn handle_incoming_client_msg(
        &mut self,
        msg: Envelope<ClientToServerMessage>,
    ) -> Result<(), ClientConnError> {
        let response = match msg.contents {
            ClientToServerMessage::WantToReserveChunks { max, strategy } => {
                let chunks = self
                    .server_state
                    .fetch_and_reserve_chunks(strategy, max, &self.tx)
                    .await?;

                self.reservations
                    .extend(chunks.iter().map(|c| (c.submission_id, c.chunk_index)));

                Some(SyncServerToClientResponse::ChunksReserved(chunks))
            }
            ClientToServerMessage::CompleteChunk { id, output_content } => {
                self.server_state.complete_chunk(id, output_content).await?;
                Some(SyncServerToClientResponse::ChunkCompleted)
            }
        };
        if let Some(response) = response {
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

    // Manages heartbeating:
    // Sends the next heartbeat, or exits with an error if too many were already missed.
    async fn beat_heart(&mut self) -> Result<(), ClientConnError> {
        if self.heartbeats_missed > 5 {
            Err(ClientConnError::HeartbeatFailure)
        } else {
            let _ = self.ws_stream.send(Message::ping("heartbeat")).await;
            self.heartbeats_missed += 1;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use http::Uri;
    use tokio::net::TcpListener;

    use crate::{
        common::submission,
        consumer::{server::ConsumerServerState, strategy::Strategy},
    };

    use super::*;

    #[sqlx::test]
    pub async fn test_handle_incoming_client_msg(pool: sqlx::SqlitePool) {
        let uri = "127.0.0.1:8081";
        let ws_uri = "ws://127.0.0.1:8081";

        let input_chunks: Vec<Option<Vec<u8>>> = vec![
            Some("1".into()),
            Some("2".into()),
            Some("3".into()),
            Some("4".into()),
        ];

        let server_state =
            ConsumerServerState::new(pool.clone(), Duration::from_secs(1), uri).await;
        let listener = TcpListener::bind(&*server_state.server_addr).await.unwrap();

        let client_handle = tokio::spawn({
            let input_chunks = input_chunks.clone();
            async move {
                let uri = Uri::from_static(ws_uri);
                let (mut client, _) = tokio_websockets::ClientBuilder::from_uri(uri)
                    .connect()
                    .await
                    .unwrap();

                let msg = client
                    .next()
                    .await
                    .expect("Should not be closed")
                    .expect("Should receive a message");
                let data: ServerToClientMessage = msg.try_into().unwrap();
                match data {
                    ServerToClientMessage::Sync(Envelope {
                        contents: SyncServerToClientResponse::ChunksReserved(chunks),
                        ..
                    }) => {
                        assert_eq!(chunks.len(), 4);
                        assert_eq!(
                            chunks
                                .iter()
                                .map(|c| c.input_content.clone())
                                .collect::<Vec<_>>(),
                            input_chunks
                        );
                    }
                    other => panic!("Unexpected message response: {other:?}"),
                }
            }
        });

        let mut conn = server_state.accept_one_conn(&listener).await.unwrap();

        let mut sql_conn = pool.acquire().await.unwrap();
        submission::insert_submission_from_chunks(None, input_chunks, &mut sql_conn)
            .await
            .unwrap();

        conn.handle_incoming_client_msg(Envelope {
            nonce: 0,
            contents: ClientToServerMessage::WantToReserveChunks {
                max: 10,
                strategy: Strategy::Oldest,
            },
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_millis(1), client_handle)
            .await
            .expect("Client ran too long; probably did not receive ws response")
            .unwrap();
    }
}

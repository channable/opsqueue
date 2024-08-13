use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use http::Uri;

use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_websockets::{ClientBuilder, Message, Payload, ServerBuilder, WebSocketStream};

use crate::common::chunk::Chunk;
use crate::consumer::common::{ClientToServerMessage, ServerToClientMessage};
use crate::consumer::strategy::Strategy;
use crate::consumer::reserver::Reserver;


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

pub struct ClientConn {
    // Websocket stream with which we communicate with the client
    ws_stream: WebSocketStream<TcpStream>,
    heartbeat_interval: tokio::time::Interval,
    heartbeats_missed: usize,
    // Channel with which the rest of the server can send messages to this particular client
    tx: UnboundedSender<Chunk>,
    rx: UnboundedReceiver<Chunk>,
    server_state: super::state::ConsumerServerState,
}


impl ClientConn {
    pub fn new(server_state: super::state::ConsumerServerState, ws_stream: WebSocketStream<TcpStream>) -> Self {
        // TODO: make interval configurable
        let heartbeat_interval = tokio::time::interval(Duration::from_secs(1));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let this = Self {
            ws_stream, heartbeat_interval, heartbeats_missed: 0, tx, rx, server_state
        };

        this
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
                Some(msg) = self.rx.recv() => {
                    // TODO
                    todo!("Server wants to send someting to client: {msg:?}");
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
    async fn handle_incoming_client_msg(&mut self, msg: ClientToServerMessage) -> Result<(), ClientConnError> {
        match msg {
            ClientToServerMessage::WantToReserveChunks { max, strategy } => {
                let chunks = self.server_state.fetch_and_reserve_chunks(strategy, max, &self.tx).await?;
                self.ws_stream.send(ServerToClientMessage::ChunksReserved(chunks).try_into()?).await?;
            }
        }

        Ok(())
    }

    // Manages heartbeating: 
    // Sends the next heartbeat, or exits with an error if too many were already missed.
    async fn beat_heart(&mut self) -> Result<(), ClientConnError> {
        if self.heartbeats_missed > 5 {
            Err(ClientConnError::HeartbeatFailure)
        } else {
            let _ = self.ws_stream.send(Message::ping("heartbeat") ).await;
            self.heartbeats_missed += 1;
            Ok(())
        }
    }
}

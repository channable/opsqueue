use std::time::Duration;

#[cfg(feature = "server-logic")]
use axum::extract::ws;

use serde::{Deserialize, Serialize};

use crate::common::chunk;
use crate::common::chunk::{Chunk, ChunkId};

use crate::common::errors::{IncorrectUsage, LimitIsZero};
use crate::common::submission::Submission;
use crate::consumer::strategy::Strategy;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum ClientToServerMessage {
    WantToReserveChunks {
        max: usize,
        strategy: Strategy,
    },
    CompleteChunk {
        id: ChunkId,
        output_content: chunk::Content,
    },
    FailChunk {
        id: ChunkId,
        failure: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerToClientMessage {
    Sync(Envelope<SyncServerToClientResponse>),
    Async(AsyncServerToClientMessage),
    /// Initialization message. The client expects to receive this as the very first message
    /// before it enters the main loop. Re-sending this message is tolerated but considered
    /// invalid.
    Init(ConsumerConfig),
}

/// Responses to earlier ClientToServerMessages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncServerToClientResponse {
    #[allow(clippy::type_complexity)]
    ChunksReserved(Result<Vec<(Chunk, Submission)>, IncorrectUsage<LimitIsZero>>),
}

/// Messages the server sends on its own
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum AsyncServerToClientMessage {
    ChunkReservationExpired(ChunkId),
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub nonce: usize,
    pub contents: T,
}

/// The part of the configuration that is shared with the consumer when it connects to
/// the opsqueue server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    /// Specifies how long the heartbeat window is. See also [Config::max_missable_heartbeats].
    ///
    /// [Config::max_missable_heartbeats]: crate::config::Config::max_missable_heartbeats
    pub max_missable_heartbeats: usize,
    /// Specifies how long the heartbeat window is. This value is determined by the
    /// [Config::heartbeat_interval] option set on the server.
    ///
    /// [Config::heartbeat_interval]: crate::config::Config::heartbeat_interval
    pub heartbeat_interval: Duration,
    /// The server includes its opsqueue library version
    /// which is mainly used for logging in the client
    /// to facilitate recognizing incorrect/potentially incompatible versions
    pub version_info: String,
    /// The connection ID. Every time that a new websocket is created from the `/consumer`
    /// endpoint, a fresh connection id is made. This is sent to the client to aid in
    /// debugging.
    pub conn_id: String,
}

#[cfg(feature = "server-logic")]
impl TryFrom<ws::Message> for Envelope<ClientToServerMessage> {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: ws::Message) -> Result<Self, Self::Error> {
        ciborium::from_reader(&*value.into_data())
    }
}

#[cfg(feature = "server-logic")]
impl TryFrom<ws::Message> for ServerToClientMessage {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: ws::Message) -> Result<Self, Self::Error> {
        ciborium::from_reader(&*value.into_data())
    }
}

#[cfg(feature = "server-logic")]
impl From<ServerToClientMessage> for ws::Message {
    fn from(val: ServerToClientMessage) -> Self {
        let mut writer = Vec::new();
        ciborium::into_writer(&val, &mut writer)
            .expect("Failed to serialize ServerToClientMessage");

        ws::Message::Binary(writer)
    }
}

#[cfg(feature = "server-logic")]
impl From<Envelope<ClientToServerMessage>> for ws::Message {
    fn from(val: Envelope<ClientToServerMessage>) -> Self {
        let mut writer = Vec::new();
        ciborium::into_writer(&val, &mut writer)
            .expect("Failed to serialize ClientToServerMessage");

        ws::Message::Binary(writer)
    }
}

// NOTE: For the time being, we have to create from/into implementations for _both_
// axum::extract::ws::Message and tokio_tungstenite::tungstenite::Message, even though the former is a wrapper for the latter.
// The reason is that axum::extract::ws intentionally hides its underlying type.
// An alternative crate called https://github.com/davidpdrsn/axum-tungstenite
// exists, but it currently is not up-to-date enough with Axum.
#[cfg(feature = "client-logic")]
impl TryFrom<tokio_tungstenite::tungstenite::Message> for Envelope<ClientToServerMessage> {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: tokio_tungstenite::tungstenite::Message) -> Result<Self, Self::Error> {
        ciborium::from_reader(&*value.into_data())
    }
}

#[cfg(feature = "client-logic")]
impl TryFrom<tokio_tungstenite::tungstenite::Message> for ServerToClientMessage {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: tokio_tungstenite::tungstenite::Message) -> Result<Self, Self::Error> {
        ciborium::from_reader(&*value.into_data())
    }
}

#[cfg(feature = "client-logic")]
impl From<ServerToClientMessage> for tokio_tungstenite::tungstenite::Message {
    fn from(val: ServerToClientMessage) -> Self {
        let mut writer = Vec::new();
        ciborium::into_writer(&val, &mut writer)
            .expect("Failed to serialize ServerToClientMessage");

        tokio_tungstenite::tungstenite::Message::Binary(writer)
    }
}

#[cfg(feature = "client-logic")]
impl From<Envelope<ClientToServerMessage>> for tokio_tungstenite::tungstenite::Message {
    fn from(val: Envelope<ClientToServerMessage>) -> Self {
        let mut writer = Vec::new();
        ciborium::into_writer(&val, &mut writer)
            .expect("Failed to serialize ClientToServerMessage");

        tokio_tungstenite::tungstenite::Message::Binary(writer)
    }
}

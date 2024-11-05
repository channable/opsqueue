use std::time::Duration;

#[cfg(feature = "server-logic")]
use axum::extract::ws;

use serde::{Deserialize, Serialize};

use crate::common::chunk;
use crate::common::chunk::{Chunk, ChunkId};

use crate::common::errors::{DatabaseError, E, IncorrectUsage, LimitIsZero};
use crate::common::submission::Submission;
use crate::consumer::strategy::Strategy;

// TODO: Make configurable
pub const MAX_MISSABLE_HEARTBEATS: usize = 3;
// TODO: Make configurable
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

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
}

/// Responses to earlier ClientToServerMessages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncServerToClientResponse {
    #[allow(clippy::type_complexity)]
    ChunksReserved(
        Result<Vec<(Chunk, Submission)>, E<DatabaseError, IncorrectUsage<LimitIsZero>>>,
    ),
    // ChunkCompleted,
    // ChunkFailed,
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

// TODO: property test ensuring serialization never panics
#[cfg(feature = "server-logic")]
impl From<ServerToClientMessage> for ws::Message {
    fn from(val: ServerToClientMessage) -> Self {
        let mut writer = Vec::new();
        ciborium::into_writer(&val, &mut writer)
            .expect("Failed to serialize ServerToClientMessage");

        ws::Message::Binary(writer)
    }
}

// TODO: property test ensuring serialization never panics
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

// TODO: property test ensuring serialization never panics
#[cfg(feature = "client-logic")]
impl From<ServerToClientMessage> for tokio_tungstenite::tungstenite::Message {
    fn from(val: ServerToClientMessage) -> Self {
        let mut writer = Vec::new();
        ciborium::into_writer(&val, &mut writer)
            .expect("Failed to serialize ServerToClientMessage");

        tokio_tungstenite::tungstenite::Message::Binary(writer)
    }
}

// TODO: property test ensuring serialization never panics
#[cfg(feature = "client-logic")]
impl From<Envelope<ClientToServerMessage>> for tokio_tungstenite::tungstenite::Message {
    fn from(val: Envelope<ClientToServerMessage>) -> Self {
        let mut writer = Vec::new();
        ciborium::into_writer(&val, &mut writer)
            .expect("Failed to serialize ClientToServerMessage");

        tokio_tungstenite::tungstenite::Message::Binary(writer)
    }
}

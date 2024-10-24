use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws;
use bytes::{Buf, BufMut, BytesMut};

use serde::{Deserialize, Serialize};


use crate::common::chunk;
use crate::common::chunk::{Chunk, ChunkId};

use crate::common::submission::Submission;
use crate::consumer::strategy::Strategy;

// TODO: Make configurable
pub const MAX_MISSABLE_HEARTBEATS: usize = 3;
// TODO: Make configurable
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

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
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum ServerToClientMessage {
    Sync(Envelope<SyncServerToClientResponse>),
    Async(AsyncServerToClientMessage),
}

/// Responses to earlier ClientToServerMessages
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum SyncServerToClientResponse {
    ChunksReserved(Vec<(Chunk, Submission)>),
    ChunkCompleted,
    ChunkFailed,
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

impl TryFrom<tokio_websockets::Message> for Envelope<ClientToServerMessage> {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: tokio_websockets::Message) -> Result<Self, Self::Error> {
        let msg: Bytes = value.into_payload().into();
        let me: Envelope<ClientToServerMessage> = ciborium::from_reader(msg.reader())?;
        Ok(me)
    }
}

impl TryFrom<tokio_websockets::Message> for ServerToClientMessage {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: tokio_websockets::Message) -> Result<Self, Self::Error> {
        let msg: Bytes = value.into_payload().into();
        let me: ServerToClientMessage = ciborium::from_reader(msg.reader())?;
        Ok(me)
    }
}

// TODO: property test ensuring serialization never panics
impl From<ServerToClientMessage> for tokio_websockets::Message {
    fn from(val: ServerToClientMessage) -> Self {
        let mut writer = BytesMut::new().writer();
        ciborium::into_writer(&val, &mut writer).expect("Failed to serialize ServerToClientMessage");

        tokio_websockets::Message::binary(writer.into_inner())
    }
}

// TODO: property test ensuring serialization never panics
impl From<Envelope<ClientToServerMessage>> for tokio_websockets::Message {
    fn from(val: Envelope<ClientToServerMessage>) -> Self {
        let mut writer = BytesMut::new().writer();
        ciborium::into_writer(&val, &mut writer).expect("Failed to serialize ClientToServerMessage");

        tokio_websockets::Message::binary(writer.into_inner())
    }
}


impl TryFrom<ws::Message> for Envelope<ClientToServerMessage> {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: ws::Message) -> Result<Self, Self::Error> {
        ciborium::from_reader(&*value.into_data())
    }
}

impl TryFrom<ws::Message> for ServerToClientMessage {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: ws::Message) -> Result<Self, Self::Error> {
        ciborium::from_reader(&*value.into_data())
    }
}

// TODO: property test ensuring serialization never panics
impl From<ServerToClientMessage> for ws::Message {
    fn from(val: ServerToClientMessage) -> Self {
        let mut writer = Vec::new();
        ciborium::into_writer(&val, &mut writer).expect("Failed to serialize ServerToClientMessage");

        ws::Message::Binary(writer)
    }
}

// TODO: property test ensuring serialization never panics
impl From<Envelope<ClientToServerMessage>> for ws::Message {
    fn from(val: Envelope<ClientToServerMessage>) -> Self {
        let mut writer = Vec::new();
        ciborium::into_writer(&val, &mut writer).expect("Failed to serialize ClientToServerMessage");

        ws::Message::Binary(writer)
    }
}

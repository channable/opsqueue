use std::time::Duration;

use axum::body::Bytes;
use bytes::{Buf, BufMut, BytesMut};

use serde::{Deserialize, Serialize};

use tokio_websockets::Message;

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

impl TryFrom<Message> for Envelope<ClientToServerMessage> {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let msg: Bytes = value.into_payload().into();
        let me: Envelope<ClientToServerMessage> = ciborium::from_reader(msg.reader())?;
        Ok(me)
    }
}

impl TryFrom<Message> for ServerToClientMessage {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let msg: Bytes = value.into_payload().into();
        let me: ServerToClientMessage = ciborium::from_reader(msg.reader())?;
        Ok(me)
    }
}

// TODO: property test ensuring serialization never panics
impl Into<Message> for ServerToClientMessage {
    fn into(self) -> Message {
        let mut writer = BytesMut::new().writer();
        ciborium::into_writer(&self, &mut writer).expect("Failed to serialize ServerToClientMessage");
        let msg = Message::binary(writer.into_inner());
        msg
    }
}

// TODO: property test ensuring serialization never panics
impl Into<Message> for Envelope<ClientToServerMessage> {
    fn into(self) -> Message {
        let mut writer = BytesMut::new().writer();
        ciborium::into_writer(&self, &mut writer).expect("Failed to serialize ClientToServerMessage");
        let msg = Message::binary(writer.into_inner());
        msg
    }
}

use axum::body::Bytes;
use bytes::{Buf, BufMut, BytesMut};

use serde::{Deserialize, Serialize};

use tokio_websockets::Message;

use crate::common::chunk::Chunk;

use crate::consumer::strategy::Strategy;
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum ClientToServerMessage {
    WantToReserveChunks { max: usize, strategy: Strategy },
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum ServerToClientMessage {
    ChunksReserved(Vec<Chunk>),
    ChunkReservationExpired {
        submission_id: i64,
        chunk_index: u32,
    },
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

impl TryFrom<Message> for Envelope<ServerToClientMessage> {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let msg: Bytes = value.into_payload().into();
        let me: Envelope<ServerToClientMessage> = ciborium::from_reader(msg.reader())?;
        Ok(me)
    }
}

impl TryInto<Message> for Envelope<ServerToClientMessage> {
    type Error = ciborium::ser::Error<std::io::Error>;
    fn try_into(self) -> Result<Message, Self::Error> {
        let mut writer = BytesMut::new().writer();
        ciborium::into_writer(&self, &mut writer)?;
        let msg = Message::binary(writer.into_inner());
        Ok(msg)
    }
}

impl TryInto<Message> for Envelope<ClientToServerMessage> {
    type Error = ciborium::ser::Error<std::io::Error>;
    fn try_into(self) -> Result<Message, Self::Error> {
        let mut writer = BytesMut::new().writer();
        ciborium::into_writer(&self, &mut writer)?;
        let msg = Message::binary(writer.into_inner());
        Ok(msg)
    }
}

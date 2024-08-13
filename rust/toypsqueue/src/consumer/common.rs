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
use crate::consumer::strategy::Strategy;
use crate::consumer::reserver::Reserver;
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum ClientToServerMessage {
  WantToReserveChunks{max: usize, strategy: Strategy},
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum ServerToClientMessage {
  ChunksReserved(Vec<Chunk>),
  ChunkReservationExpired{submission_id: i64, chunk_index: u32},
}


impl TryFrom<Message> for ClientToServerMessage {
    type Error = ciborium::de::Error<std::io::Error>;
    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let msg: Bytes = value.into_payload().into();
        let me: ClientToServerMessage = ciborium::from_reader(msg.reader())?;
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

impl TryInto<Message> for ServerToClientMessage {
    type Error = ciborium::ser::Error<std::io::Error>;
    fn try_into(self) -> Result<Message, Self::Error> {
        let mut writer = BytesMut::new().writer();
        ciborium::into_writer(&self, &mut writer)?;
        let msg = Message::binary(writer.into_inner());
        Ok(msg)
    }
}

impl TryInto<Message> for ClientToServerMessage {
    type Error = ciborium::ser::Error<std::io::Error>;
    fn try_into(self) -> Result<Message, Self::Error> {
        let mut writer = BytesMut::new().writer();
        ciborium::into_writer(&self, &mut writer)?;
        let msg = Message::binary(writer.into_inner());
        Ok(msg)
    }
}

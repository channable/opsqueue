use std::ops::Deref;
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
use super::strategy::Strategy;
use super::reserver::Reserver;


/// State for the consumer-side of the server.
/// Cloning this structure is cheap, as its contents are Arc-like, 
/// with its internal (mutable, thread-safe) state being shared between clones.
#[derive(Debug, Clone)]
pub struct ConsumerServerState {
    pool: sqlx::SqlitePool,
    // reservation_expiration: Duration,
    reserver: Reserver<i64, Chunk>,
}

impl ConsumerServerState {
    pub async fn new(pool: sqlx::SqlitePool, reservation_expiration: Duration) -> Self {
        let reserver = Reserver::new(reservation_expiration);
        ConsumerServerState {
            pool,
            reserver,
            // reservation_expiration,
        }
    }

    /// Select chunks, and store them in the reserver, to make sure that re-running the same selection returns different results as long as they are reserved.
    ///
    /// `query_fun` is a 'strategy' function returning a stream of chunks, something like `sqlx::query!("SELECT * FROM chunks WHERE ...").fetch(conn)`
    /// `limit` is the maximum number of chunks to return. The query/stream will be evaluated until that many not-already-reserved chunks can be returned.
    /// `stale_chunks_notifier` is a Tokio channel, which the reserver will automatically invoke when a particular chunk reservation has expired.
    async fn reserve_chunks(
        &self,
        stream: impl Stream<Item = Result<Chunk, sqlx::Error>>,
        limit: usize,
        stale_chunks_notifier: &tokio::sync::mpsc::UnboundedSender<Chunk>,
    ) -> Result<Vec<Chunk>, sqlx::Error> {
        stream
            .try_filter_map(|chunk| async move {
                Ok(self
                    .reserver
                    .try_reserve(chunk.id, chunk, stale_chunks_notifier))
            })
            .take(limit)
            .try_collect()
            .await
    }

    pub async fn fetch_and_reserve_chunks(
        &self,
        strategy: Strategy,
        limit: usize,
        stale_chunks_notifier: &tokio::sync::mpsc::UnboundedSender<Chunk>,
    ) -> Result<Vec<Chunk>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        let stream = strategy.execute(&mut conn);
        self.reserve_chunks(stream, limit, stale_chunks_notifier)
            .await
    }

    pub async fn run(self) -> anyhow::Result<()> {
        println!("Running server");
        let listener = TcpListener::bind("127.0.0.1:3333").await?;
        println!("Listener listens");
        while let Ok((stream, _)) = listener.accept().await {
            println!("Opening new connection");
            let ws_stream = ServerBuilder::new().accept(stream).await?;
            println!("Stream made");
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            tokio::spawn(self.run_conn(ws_stream, rx));
            tx.send(42).await?;
            tx.send(69).await?;
            todo!()
            // store tx for later use
        }
        Ok(())
    }

    pub async fn run_conn(
        self,
        mut consumer_conn: WebSocketStream<TcpStream>,
        mut receiver: tokio::sync::mpsc::Receiver<usize>,
    ) -> anyhow::Result<()> {
        println!("Server runs consumer conn");
        let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                val = consumer_conn.next() => {
                    match val {
                        None => break,
                        Some(Err(_)) => break,
                        Some(Ok(msg)) => {
                            if msg.is_text() {
                                let x: &str = msg.as_text().unwrap();
                                println!("Server received: {x:?}");
                                consumer_conn.send(Message::text(format!("{x}!"))).await?;
                            }
                        }
                    }
                },
                Some(val) = receiver.recv() => {
                    println!("Received message from app: {val:?}");
                    consumer_conn.send(Message::text(format!("{val}!"))).await?;
                }
                _ = heartbeat_interval.tick() => {
                    println!("Tick");
                }
            }
        }
        Ok(())

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
    server_state: ConsumerServerState,
}


#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum ClientToServerMessage {
  WantToReserveChunks{max: usize, strategy: Strategy},
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum ServerToClientMessage {
  ChunksReserved(Vec<Chunk>),
  ChunkReservationExpired{submission_id: i64, chunk_index: u32},
}

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

impl ClientConn {
    pub fn new(server_state: ConsumerServerState, ws_stream: WebSocketStream<TcpStream>) -> Self {
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
            let msg: Bytes = msg.into_payload().into();
            let msg: ClientToServerMessage = ciborium::from_reader(msg.reader())?;
            self.handle_incoming_client_msg(msg).await?;
        }

        Ok(())
    }

    // Deals with app-specific messages arrived through the Websocket connection.
    async fn handle_incoming_client_msg(&mut self, msg: ClientToServerMessage) -> Result<(), ClientConnError> {
        match msg {
            ClientToServerMessage::WantToReserveChunks { max, strategy } => {
                let chunks = self.server_state.fetch_and_reserve_chunks(strategy, max, &self.tx).await?;

                // TODO extract serializing to helper function on ServerToClientMessage
                let mut writer = BytesMut::new().writer();
                ciborium::into_writer(&ServerToClientMessage::ChunksReserved(chunks), &mut writer)?;
                let msg = Message::binary(writer.into_inner());
                self.ws_stream.send(msg).await?;
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


#[cfg(test)]
mod tests {

    use super::*;
    use crate::common::chunk::Chunk;

    #[sqlx::test]
    pub async fn test_fetch_and_reserve_chunks(pool: sqlx::SqlitePool) {
        // let cleanup_fun = |chunk| { println!("Cleaning up chunk: {:?}", chunk); };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::task::spawn(async move {
            while let Some(chunk) = rx.recv().await {
                println!("Cleaning up chunk: {:?}", chunk)
            }
        });

        let state = ConsumerServerState::new(pool.clone(), Duration::from_secs(1)).await;
        // let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        let zero = Chunk::new(1, 0, vec![1, 2, 3]);
        let one = Chunk::new(1, 1, vec![1, 2, 3]);
        let two = Chunk::new(1, 2, vec![1, 2, 3]);
        let three = Chunk::new(1, 3, vec![1, 2, 3]);
        let four = Chunk::new(1, 4, vec![1, 2, 3]);
        let chunks = vec![
            zero.clone(),
            one.clone(),
            two.clone(),
            three.clone(),
            four.clone(),
        ];
        crate::common::chunk::insert_many_chunks(chunks.clone(), pool.acquire().await.unwrap())
            .await
            .unwrap();

        // let fun = crate::chunk::select_oldest_chunks_stream2;
        // let out = state.fetch_and_reserve_chunks(move |conn| {
        //     // fun(conn)
        //     crate::chunk::select_oldest_chunks_stream(conn)
        // }, 3, &tx).await.unwrap();
        let out = state
            .fetch_and_reserve_chunks(Strategy::Oldest, 3, &tx)
            .await
            .unwrap();
        assert_eq!(out, vec![zero, one, two]);

        let out2 = state
            .fetch_and_reserve_chunks(Strategy::Oldest, 3, &tx)
            .await
            .unwrap();
        assert_eq!(out2, vec![three, four]);
    }
}

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



/// State for the consumer-side of the server.
/// Cloning this structure is cheap, as its contents are Arc-like, 
/// with its internal (mutable, thread-safe) state being shared between clones.
#[derive(Debug, Clone)]
pub struct ConsumerServerState {
    pool: sqlx::SqlitePool,
    reserver: Reserver<i64, Chunk>,
    pub server_addr: Arc<str>,
}

impl ConsumerServerState {
    pub async fn new(pool: sqlx::SqlitePool, reservation_expiration: Duration, server_addr: &str) -> Self {
        let reserver = Reserver::new(reservation_expiration);
        ConsumerServerState {
            pool,
            reserver,
            server_addr: Arc::from(server_addr)
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

    /// Used for testing
    pub async fn accept_one_conn(&self, listener: &TcpListener) -> anyhow::Result<super::conn::ClientConn> {
        let (stream, _addr) = listener.accept().await?;
        let ws_stream = ServerBuilder::new().accept(stream).await?;
        Ok(super::conn::ClientConn::new(self.clone(), ws_stream))
    }

    pub async fn run(self) -> anyhow::Result<()> {
        println!("Running server");
        let listener = TcpListener::bind(&*self.server_addr).await?;
        println!("Listener listens");
        while let Ok(conn) = self.accept_one_conn(&listener).await {
            tokio::spawn(conn.run());

        }
        Ok(())
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

        let url = "http://localhost:3333";

        let state = ConsumerServerState::new(pool.clone(), Duration::from_secs(1), url).await;
        // let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        let zero = Chunk::new(1, 0, None);
        let one = Chunk::new(1, 1, None);
        let two = Chunk::new(1, 2, None);
        let three = Chunk::new(1, 3, None);
        let four = Chunk::new(1, 4, None);
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

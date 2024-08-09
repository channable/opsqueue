pub mod reserver;
pub mod strategy;

use std::time::Duration;

use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use http::Uri;

use tokio::net::{TcpListener, TcpStream};
use tokio_websockets::{ClientBuilder, Message, ServerBuilder, WebSocketStream};

use crate::common::chunk::Chunk;
use strategy::Strategy;
use reserver::Reserver;

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
}

// pub async fn reserve_chunks(desired_chunks: impl IntoIterator<Item = Chunk>, cleanup_fun: CleanupFun<Chunk>, reserver: &Reserver<i64, Chunk>) -> Vec<Chunk>
// {
//     let res = desired_chunks.into_iter().map(|chunk| {
//         reserver.try_reserve(chunk.id, chunk, cleanup_fun.clone())
//     }).flatten().collect();
//     reserver.run_pending_tasks();
//     res
// }

pub async fn run_consumer_server() -> anyhow::Result<()> {
    println!("Running server");
    let listener = TcpListener::bind("127.0.0.1:3333").await?;
    println!("Listener listens");
    while let Ok((stream, _)) = listener.accept().await {
        println!("Opening new connection");
        let ws_stream = ServerBuilder::new().accept(stream).await?;
        println!("Stream made");
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(run_consumer_conn(ws_stream, rx));
        tx.send(42).await?;
        tx.send(69).await?;
        todo!()
        // store tx for later use
    }
    Ok(())
}

pub async fn run_consumer_conn(
    mut consumer_conn: WebSocketStream<TcpStream>,
    mut receiver: tokio::sync::mpsc::Receiver<usize>,
) -> anyhow::Result<()> {
    println!("Server runs consumer conn");
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

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
            _ = interval.tick() => {
                println!("Tick");
            }
        }
    }
    // while let Some(Ok(msg)) = consumer_conn.next().await {
    //     if msg.is_text() {
    //         let x: &str = msg.as_text().unwrap();
    //         println!("Server received: {x:?}");
    //         consumer_conn.send(Message::text(format!("{x}!"))).await?;
    //     }
    // }
    Ok(())
}

pub async fn run_consumer_client() -> anyhow::Result<()> {
    println!("Running client");
    let uri = Uri::from_static("ws://127.0.0.1:3333");
    let (mut client, _) = ClientBuilder::from_uri(uri).connect().await?;
    println!("Client built");

    // client.send(Message::text("Hello world!")).await?;
    println!("First message sent");

    while let Some(Ok(msg)) = client.next().await {
        if msg.is_text() {
            let x = msg.as_text().unwrap();
            println!("Client Received: {x:?}");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            client.send(msg).await?;
        }
    }

    Ok(())
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

// #[tokio::test(flavor = "multi_thread")]
// async fn ping_pong() {
//     let a = tokio::spawn(run_consumer_server());
//     let b = tokio::spawn(run_consumer_client());
//     let _ = tokio::join!(a, b);
// }

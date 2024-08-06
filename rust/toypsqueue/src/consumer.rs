use futures::{SinkExt, StreamExt, TryStreamExt};
use http::Uri;
use itertools::Itertools;
use tokio::net::{TcpListener, TcpStream};
use tokio_websockets::{ClientBuilder, Message, ServerBuilder, WebSocketStream};

use crate::{chunk::Chunk, reserver::Reserver};

#[derive(Debug, Clone)]
pub struct ServerState<CleanupFun: FnOnce(Chunk) + Clone + Send + Sync + 'static> {
    pool: sqlx::SqlitePool,
    reserver: Reserver<i64, Chunk, CleanupFun>,
}

impl<CleanupFun: FnOnce(Chunk) + Clone + Send + Sync + 'static> ServerState<CleanupFun> {
    pub async fn new(pool: sqlx::SqlitePool) -> Self {
        let reserver = Reserver::new();
        // let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        ServerState{reserver, pool}
    }
}

pub async fn foo<T: Fn(Chunk) + Copy + Send + Sync + 'static>(state: &ServerState<T>, cleanup_fun: T) -> Vec<Chunk> {
    let iter = super::chunk::select_oldest_chunks(&state.pool, 3).await;
    let res = reserve_chunks(iter, cleanup_fun, &state.reserver).await;
    res
}

pub async fn foo_stream<T: Fn(Chunk) + Copy + Send + Sync + 'static>(state: &ServerState<T>, cleanup_fun: T, limit: usize) -> Result<Vec<Chunk>, sqlx::Error> {
    let stream = super::chunk::select_oldest_chunks_stream(&state.pool);
    // let res = reserve_chunks_stream(iter, cleanup_fun, &state.reserver).await;
    let res = stream.try_filter_map(|chunk| async move {
        Ok(state.reserver.try_reserve(chunk.id, chunk, cleanup_fun))
    }).take(limit).try_collect().await;
    res
}

#[cfg(test)]
mod tests {
    use crate::chunk::Chunk;
    use super::*;

    #[sqlx::test]
    pub async fn test_foo(pool: sqlx::SqlitePool) {
        let cleanup_fun = |chunk| { println!("Cleaning up chunk: {:?}", chunk); };

        let state = ServerState::new(pool.clone()).await;
        // let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        let zero = Chunk::new(1,0,vec![1,2,3]);
        let one = Chunk::new(1,1,vec![1,2,3]);
        let two = Chunk::new(1,2,vec![1,2,3]);
        let three = Chunk::new(1,3,vec![1,2,3]);
        let chunks = vec![zero.clone(), one.clone(), two.clone(), three.clone()];
        crate::chunk::insert_many_chunks(chunks.clone(), pool.acquire().await.unwrap()).await.unwrap();

        let out = foo_stream(&state, cleanup_fun, 3).await.unwrap();
        assert_eq!(out, vec![zero, one, two]);

        let out2 = foo_stream(&state, cleanup_fun, 3).await.unwrap();
        assert_eq!(out2, vec![three]);
    }
}


pub async fn reserve_chunks<CleanupFun>(desired_chunks: impl IntoIterator<Item = Chunk>, cleanup_fun: CleanupFun, reserver: &Reserver<i64, Chunk, CleanupFun>) -> Vec<Chunk> 
    where CleanupFun: Fn(Chunk) + Sync + Send + Copy + 'static,
{
    let res = desired_chunks.into_iter().map(|chunk| {
        reserver.try_reserve(chunk.id, chunk, cleanup_fun)
    }).flatten().collect();
    reserver.run_pending_tasks();
    res
}

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

// #[tokio::test(flavor = "multi_thread")]
// async fn ping_pong() {
//     let a = tokio::spawn(run_consumer_server());
//     let b = tokio::spawn(run_consumer_client());
//     let _ = tokio::join!(a, b);
// }

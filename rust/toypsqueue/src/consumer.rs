pub mod client;
pub mod common;
pub mod reserver;
pub mod server;
pub mod strategy;

use futures::{SinkExt, StreamExt};
use http::Uri;

use tokio::net::{TcpListener, TcpStream};
use tokio_websockets::{ClientBuilder, Message, ServerBuilder, WebSocketStream};

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

// #[tokio::test(flavor = "multi_thread")]
// async fn ping_pong() {
//     let a = tokio::spawn(run_consumer_server());
//     let b = tokio::spawn(run_consumer_client());
//     let _ = tokio::join!(a, b);
// }

use std::{collections::HashMap, str::FromStr, sync::Arc};

use futures::{SinkExt, StreamExt};
use http::Uri;
use tokio::{net::TcpStream, select, sync::{mpsc, oneshot, Mutex}};
use tokio_websockets::{ClientBuilder, MaybeTlsStream, Message, WebSocketStream};

use crate::common::chunk::Chunk;

use super::{common::{ClientToServerMessage, Envelope, ServerToClientMessage}, strategy::Strategy};


#[derive(Debug)]
pub struct ClientInner {
    // ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    // web_socket_stream_handler: Mutex<WebSocketStreamHandler>,
    tx: mpsc::UnboundedSender<Message>,
    rx: Mutex<mpsc::UnboundedReceiver<Result<Message, tokio_websockets::Error>>>,

    // A map of in-flight requests, containing response channels to have async code continue running once the response comes in.
    // The map's values also contains the sent request for debugging. (We might change that in the future, or change it based on config params)
    in_flight_requests: Mutex<HashMap<usize, (ClientToServerMessage, oneshot::Sender<ServerToClientMessage>)>>,
}

#[derive(Debug, Clone)]
pub struct Client(Arc<ClientInner>);

impl Client {
    pub async fn build_and_run(url: &str) -> anyhow::Result<Self> {
        let uri = Uri::from_str(url)?;
        let in_flight_requests = Mutex::new(HashMap::new());

        let (ws_stream, _resp) = ClientBuilder::from_uri(uri).connect().await?;
        let (web_socket_stream_handler, rx, tx) = WebSocketStreamHandler::new(ws_stream).await;


        let me = Client(Arc::new(ClientInner { rx: Mutex::new(rx), tx, in_flight_requests}));
        tokio::spawn(web_socket_stream_handler.run());
        tokio::spawn({
            let me = me.clone();
            async move { me.run_in_background().await }
        });

        Ok(me)
    }

    pub async fn reserve_chunks(&self, max: usize, strategy: Strategy) -> anyhow::Result<Vec<Chunk>> {
        let resp = self.request(ClientToServerMessage::WantToReserveChunks { max, strategy }).await?;
        match resp {
            ServerToClientMessage::ChunksReserved(chunks) => Ok(chunks),
            _ => anyhow::bail!("Unexpected response from server: {:?}", resp),
        }
    }

    async fn request(&self, request: ClientToServerMessage) -> anyhow::Result<ServerToClientMessage> {
        let (tx, mut rx) = oneshot::channel();
        {
            let mut in_flight_requests = self.0.in_flight_requests.lock().await;
            let request_id = in_flight_requests.len();
            in_flight_requests.insert(request_id, (request.clone(), tx));
            let envelope = Envelope { nonce: request_id, contents: request };
            let _ = self.0.tx.send(envelope.try_into()?);
        }

        let resp = rx.await?;

        Ok(resp)
    }

    /// This method should be started once the client is constructed. It makes sure that responses are processed when they come in from the server.
    /// 
    /// TODO: Handle expiring chunk reservations.
    pub async fn run_in_background(&self) -> anyhow::Result<()> {

        loop {
            println!("Waiting for resp");
            let resp = self.0.rx.lock().await.recv().await;
            dbg!(&resp);
            let mut in_flight_requests = self.0.in_flight_requests.lock().await;
            dbg!(&in_flight_requests);
            match resp {
                // Connection closed correctly
                None if in_flight_requests.is_empty() => return Ok(()),
                // Connection closed before last request was  handled
                None => anyhow::bail!("Connection closed unexpectedly. The following requests were still in flight: {:?}", in_flight_requests),
                // Returning malformed data:
                Some(Err(problem)) => return Err(problem.into()),
                Some(Ok(msg)) => {
                    let val: Envelope<ServerToClientMessage> = msg.try_into()?;
                    let (_request, response_channel) = in_flight_requests.remove(&val.nonce).ok_or(anyhow::anyhow!("No request found in in-flight requests; ID: {val:?}"))?;
                    response_channel.send(val.contents).map_err(|err| anyhow::anyhow!("Cannot handle response {err:?}. Response receiver was already dropped"))?;
                }
            }
        }
    }
}

/// Wraps a `WebSocketStream` in a way where the `rx` and `tx` can separately be managed,
/// which allows having a background task receiving messages while other tasks are sending messages.
/// 
/// NOTE: This code feels quite complicated/hacky. A prime example of something to clean up in a production implementation of Opsqueue.
pub struct WebSocketStreamHandler {
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    sender: mpsc::UnboundedSender<Result<Message, tokio_websockets::Error>>,
    receiver: mpsc::UnboundedReceiver<Message>,
}

impl WebSocketStreamHandler {
    pub async fn new(ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>) -> (Self, mpsc::UnboundedReceiver<Result<Message, tokio_websockets::Error>>, mpsc::UnboundedSender<Message>) {
        let (sender_tx, sender_rx) = mpsc::unbounded_channel();
        let (receiver_tx, receiver_rx) = mpsc::unbounded_channel();
        (Self { ws_stream, sender: sender_tx, receiver: receiver_rx}, sender_rx, receiver_tx)
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        println!("Websocket StreamHandler started");
        loop {
            println!("Iteration");
            select! {
                Some(msg) = self.ws_stream.next() => {
                    self.sender.send(msg).map_err(|_| anyhow::anyhow!("Sender closed unexpectedly"))?
                },
                Some(msg) = self.receiver.recv() => {
                    self.ws_stream.send(msg).await?
                },
                // else => anyhow::bail!("WebSocket connection or receiver closed unexpectedly"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::task::yield_now;

    use crate::consumer::server::ConsumerServerState;

    use super::*;

    #[sqlx::test]
    pub async fn test_fetch_chunks(pool: sqlx::SqlitePool) {
        let uri = "127.0.0.1:10082";
        let ws_uri = "ws://127.0.0.1:10082";

        let mut conn = pool.acquire().await.unwrap();
        let input_chunks = vec![Some("a".into()), Some("b".into()), Some("c".into())];
        crate::common::submission::insert_submission_from_chunks(None, input_chunks.clone(), &mut *conn).await.unwrap();

        let _server_handle = tokio::spawn(ConsumerServerState::new(pool.clone(), Duration::from_secs(60), uri).await.run());

        yield_now().await;


        let client = Client::build_and_run(ws_uri).await.unwrap();
        println!("A");

        let chunks = client.reserve_chunks(3, Strategy::Oldest).await.unwrap();
        println!("Hello");

        assert_eq!(chunks.iter().map(|c| c.input_content.clone()).collect::<Vec<Option<Vec<u8>>>>(), input_chunks);
    }
}

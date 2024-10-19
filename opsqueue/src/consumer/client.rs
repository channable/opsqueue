use std::{collections::HashMap, str::FromStr, sync::Arc};

use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use http::Uri;
use tokio::net::TcpStream;
use tokio::{
    select,
    sync::{oneshot, Mutex},
    task::yield_now,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tokio_websockets::{MaybeTlsStream, Message, WebSocketStream};

use crate::{
    common::chunk::{self, Chunk, ChunkId},
    consumer::common::{AsyncServerToClientMessage, Envelope},
};

use super::{
    common::{ClientToServerMessage, ServerToClientMessage, SyncServerToClientResponse},
    strategy::Strategy,
};

type InFlightRequests = Arc<
    Mutex<(
        usize,
        HashMap<usize, oneshot::Sender<SyncServerToClientResponse>>,
    )>,
>;
type WebsocketTcpStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
#[derive(Debug, Clone)]
pub struct Client {
    in_flight_requests: InFlightRequests,
    ws_sink: Arc<Mutex<SplitSink<WebsocketTcpStream, Message>>>,
    _bg_handle: Arc<DropGuard>,
}

impl Client {
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let uri = Uri::from_str(url)?;
        let in_flight_requests: InFlightRequests = Arc::new(Mutex::new((0, HashMap::new())));

        let (websocket_conn, _resp) = tokio_websockets::ClientBuilder::from_uri(uri)
            .connect()
            .await?;
        let (ws_sink, ws_stream) = websocket_conn.split();
        let cancellation_token = CancellationToken::new();

        tokio::spawn(Self::background_task(
            cancellation_token.clone(),
            in_flight_requests.clone(),
            ws_stream,
        ));

        let me = Self {
            in_flight_requests,
            _bg_handle: Arc::from(cancellation_token.drop_guard()),
            ws_sink: Arc::from(Mutex::new(ws_sink)),
        };
        Ok(me)
    }

    async fn background_task(
        cancellation_token: CancellationToken,
        in_flight_requests: InFlightRequests,
        mut ws_stream: SplitStream<WebsocketTcpStream>,
    ) {
        loop {
            yield_now().await;
            select! {
                _ = cancellation_token.cancelled() => break,
                Some(msg) = ws_stream.next() => {
                    let msg = msg.unwrap();
                    if msg.is_ping() {
                        log::warn!("Received Heartbeat. (TODO: Handle)");
                    } else {
                        let msg: ServerToClientMessage = msg.try_into().expect("Unparseable ServerToClientMessage");
                        match msg {
                            ServerToClientMessage::Sync(envelope) => {
                                let mut in_flight_requests = in_flight_requests.lock().await;
                                // Handle the response to some earlier request
                                let oneshot_receiver = in_flight_requests.1.remove(&envelope.nonce).expect("Received response with nonce that matches none of the open requests");
                                let _ = oneshot_receiver.send(envelope.contents);

                            },
                            ServerToClientMessage::Async(msg) => {
                                // Handle a message from the server that was not associated with an earlier request
                                match msg {
                                    AsyncServerToClientMessage::ChunkReservationExpired(_chunk_id) => {
                                        log::warn!("TODO: Client should cancel execution of current work if possible");
                                    },
                                }
                            }
                        }
                    }
                },
            }
        }
    }

    async fn request(
        &self,
        request: ClientToServerMessage,
    ) -> anyhow::Result<SyncServerToClientResponse> {
        let (oneshot_sender, oneshot_receiver) = oneshot::channel();
        {
            let mut in_flight_requests = self.in_flight_requests.lock().await;
            let nonce = in_flight_requests.0.wrapping_add(1);
            let envelope = Envelope {
                nonce: nonce,
                contents: request,
            };
            in_flight_requests.1.insert(nonce, oneshot_sender);
            let _ = self
                .ws_sink
                .lock()
                .await
                .send(envelope.into())
                .await;
        }
        let resp = oneshot_receiver.await?;
        Ok(resp)
    }

    pub async fn reserve_chunks(
        &self,
        max: usize,
        strategy: Strategy,
    ) -> anyhow::Result<Vec<Chunk>> {
        let resp = self
            .request(ClientToServerMessage::WantToReserveChunks { max, strategy })
            .await?;
        match resp {
            SyncServerToClientResponse::ChunksReserved(chunks) => Ok(chunks),
            _ => anyhow::bail!("Unexpected response from server: {:?}", resp),
        }
    }

    pub async fn complete_chunk(
        &self,
        id: ChunkId,
        output_content: chunk::Content,
    ) -> anyhow::Result<()> {
        let resp = self
            .request(ClientToServerMessage::CompleteChunk { id, output_content })
            .await?;
        match resp {
            SyncServerToClientResponse::ChunkCompleted => Ok(()),
            _ => anyhow::bail!("Unexpected response from server: {:?}", resp),
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
        let uri = "0.0.0.0:10083";
        let ws_uri = "ws://0.0.0.0:10083";

        let mut conn = pool.acquire().await.unwrap();
        let input_chunks = vec![
            Some("a".into()),
            Some("b".into()),
            Some("c".into()),
            Some("d".into()),
            Some("e".into()),
        ];
        crate::common::submission::insert_submission_from_chunks(
            None,
            input_chunks.clone(),
            &mut conn,
        )
        .await
        .unwrap();

        let _server_handle = tokio::spawn(
            ConsumerServerState::new(pool.clone(), Duration::from_secs(60), uri)
                .await
                .run(),
        );

        yield_now().await;

        let client = Client::new(ws_uri).await.unwrap();
        yield_now().await;

        let chunks = client.reserve_chunks(3, Strategy::Oldest).await.unwrap();
        yield_now().await;

        assert_eq!(
            chunks
                .iter()
                .map(|c| c.input_content.clone())
                .collect::<Vec<Option<Vec<u8>>>>(),
            input_chunks[0..3]
        );

        let two = client.reserve_chunks(3, Strategy::Oldest);
        let three = client.reserve_chunks(3, Strategy::Oldest);

        yield_now().await;

        let three = three.await;
        let two = two.await;
    }
}

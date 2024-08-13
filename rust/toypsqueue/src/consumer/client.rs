use std::str::FromStr;

use futures::{SinkExt, StreamExt};
use http::Uri;
use tokio::net::TcpStream;
use tokio_websockets::{ClientBuilder, MaybeTlsStream, WebSocketStream};

use crate::common::chunk::Chunk;

use super::{common::{ClientToServerMessage, ServerToClientMessage}, strategy::Strategy};

// TODO: Proper multiplexing of requests/responses
// And with that, proper handling of expired reservations
pub struct Client{
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Client {
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let uri = Uri::from_str(url)?;
        let (client, _upgrade_response) = ClientBuilder::from_uri(uri).connect().await?;
         Ok(Client{ws_stream: client})
    }

    pub async fn reserve_chunks(&mut self, max: usize, strategy: Strategy) -> anyhow::Result<Vec<Chunk>> {
        self.ws_stream.send(ClientToServerMessage::WantToReserveChunks { max, strategy }.try_into()?).await?;

        match self.ws_stream.next().await {
            // Connection closed:
            None => Err(anyhow::anyhow!("Connection closed unexpectedly")),
            // Incorrect protocol usage: 
            Some(Err(problem)) => Err(problem.into()),
            // Message received:
            Some(Ok(msg)) => {
                let resp: ServerToClientMessage = msg.try_into()?;
                match resp {
                    ServerToClientMessage::ChunksReserved(chunks) => Ok(chunks),
                    _ => Err(anyhow::anyhow!("Unexpected message received: {:?}", resp)),
                }
            }
        }
    }
}

use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt, TryStreamExt};

use tokio::net::TcpListener;

use tokio_websockets::ServerBuilder;

use crate::common::chunk::{self, Chunk, ChunkId};
use crate::consumer::reserver::Reserver;
use crate::consumer::strategy::Strategy;

/// State for the consumer-side of the server.
/// Cloning this structure is cheap, as its contents are Arc-like,
/// with its internal (mutable, thread-safe) state being shared between clones.
#[derive(Debug, Clone)]
pub struct ConsumerServerState {
    pool: sqlx::SqlitePool,
    reserver: Reserver<ChunkId, ChunkId>,
    pub server_addr: Arc<str>,
}

impl ConsumerServerState {
    pub async fn new(
        pool: sqlx::SqlitePool,
        reservation_expiration: Duration,
        server_addr: &str,
    ) -> Self {
        let reserver = Reserver::new(reservation_expiration);
        ConsumerServerState {
            pool,
            reserver,
            server_addr: Arc::from(server_addr),
        }
    }

    /// Select chunks, and store them in the reserver, to make sure that re-running the same selection returns different results as long as they are reserved.
    ///
    /// - `query_fun` is a 'strategy' function returning a stream of chunks, something like `sqlx::query!("SELECT * FROM chunks WHERE ...").fetch(conn)`
    /// - `limit` is the maximum number of chunks to return. The query/stream will be evaluated until that many not-already-reserved chunks can be returned.
    ///    Of course, we'll return less if the stream is exhausted before `limit` is reached.
    /// - `stale_chunks_notifier` is a Tokio channel, which the reserver will automatically invoke when a particular chunk reservation has expired.
    async fn reserve_chunks(
        &self,
        stream: impl Stream<Item = Result<Chunk, sqlx::Error>>,
        limit: usize,
        stale_chunks_notifier: &tokio::sync::mpsc::UnboundedSender<ChunkId>,
    ) -> Result<Vec<Chunk>, sqlx::Error> {
        stream
            .try_filter_map(|chunk| async move {
                let chunk_id = (chunk.submission_id, chunk.chunk_index);
                Ok(self
                    .reserver
                    .try_reserve(chunk_id, chunk_id, stale_chunks_notifier)
                    .map(|_| chunk))
            })
            .take(limit)
            .try_collect()
            .await
    }

    pub async fn fetch_and_reserve_chunks(
        &self,
        strategy: Strategy,
        limit: usize,
        stale_chunks_notifier: &tokio::sync::mpsc::UnboundedSender<ChunkId>,
    ) -> Result<Vec<Chunk>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        let stream = strategy.execute(&mut conn);
        self.reserve_chunks(stream, limit, stale_chunks_notifier)
            .await
    }

    pub async fn complete_chunk(
        &self,
        id: ChunkId,
        output_content: chunk::Content,
    ) -> Result<(), sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        let res = chunk::complete_chunk(id, output_content, &mut conn).await?;
        // TODO: Double-check if this cleanup logic is correct.
        // i.e. if the query fails, we currently keep the reservation but is this correct?
        self.reserver.finish_reservation(&id);
        Ok(res)
    }

    pub(crate) async fn accept_one_conn(
        &self,
        listener: &TcpListener,
    ) -> anyhow::Result<super::conn::ClientConn> {
        println!("Waitning for a WS connection...");
        let (stream, addr) = listener.accept().await?;
        println!("Incoming consumer client HTTP connection from {}", &addr);
        let ws_stream = ServerBuilder::new().accept(stream).await?;
        println!("HTTP-> WS upgrade succeeded for {}", &addr);
        Ok(super::conn::ClientConn::new(self.clone(), ws_stream))
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&*self.server_addr).await?;
        println!(
            "Consumer Websocket server listening at {}",
            self.server_addr
        );
        while let Ok(conn) = self.accept_one_conn(&listener).await {
            tokio::spawn(conn.run());
        }
        Ok(())
    }

    pub fn finish_reservation(&self, chunk_id: &ChunkId) {
        self.reserver.finish_reservation(chunk_id);
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
        let submission_id = 1.into();
        let zero = Chunk::new(submission_id, 0.into(), None);
        let one = Chunk::new(submission_id, 1.into(), None);
        let two = Chunk::new(submission_id, 2.into(), None);
        let three = Chunk::new(submission_id, 3.into(), None);
        let four = Chunk::new(submission_id, 4.into(), None);
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

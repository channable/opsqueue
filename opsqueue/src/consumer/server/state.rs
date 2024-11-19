use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use axum_prometheus::metrics::histogram;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::common::chunk;
use crate::common::errors::DatabaseError;
use crate::consumer::strategy::ChunkStream;
use crate::consumer::strategy::Strategy;
use crate::{
    common::{
        chunk::{Chunk, ChunkId},
        submission::Submission,
    },
    consumer::reserver::Reserver,
};

use super::CompleterMessage;
use super::ServerState;
use crate::common::errors::{IncorrectUsage, LimitIsZero, E};

// TODO: We currently clone the arc-like pool and reserver,
// but we could probably just give this struct a lifetime,
// as it will never outlive the ServerState it is created from.
#[derive(Debug, Clone)]
pub struct ConsumerState {
    // NOTE: This is an arc-like field, referring to the same DB pool.
    pool: sqlx::SqlitePool,
    // NOTE: This is an arc-like field, referring to the same reserver as all other ConsumerStates.
    reserver: Reserver<ChunkId, ChunkId>,
    // The following are the consumer-specific chunks that are currently reserved.
    reservations: Arc<Mutex<HashSet<ChunkId>>>,
    server_state: Arc<ServerState>,
}

impl Drop for ConsumerState {
    fn drop(&mut self) {
        let reservations = self.reservations.lock().unwrap();
        for reservation in &*reservations {
            // We're not tracking chunk durations that are unreserved during consumer shutdown,
            // as those will be by definition unfinished
            let _ = self.reserver.finish_reservation_sync(reservation);
        }
    }
}

impl ConsumerState {
    pub fn new(server_state: &Arc<ServerState>) -> Self {
        Self {
            pool: server_state.pool.read_pool.clone(),
            reserver: server_state.reserver.clone(),
            reservations: Arc::new(Mutex::new(HashSet::new())),
            server_state: server_state.clone(),
        }
    }

    /// Select chunks, and store them in the reserver, to make sure that re-running the same selection returns different results as long as they are reserved.
    ///
    /// - `query_fun` is a 'strategy' function returning a stream of chunks, something like `sqlx::query!("SELECT * FROM chunks WHERE ...").fetch(conn)`
    /// - `limit` is the maximum number of chunks to return. The query/stream will be evaluated until that many not-already-reserved chunks can be returned.
    ///    Of course, we'll return less if the stream is exhausted before `limit` is reached.
    /// - `stale_chunks_notifier` is a Tokio channel, which the reserver will automatically invoke when a particular chunk reservation has expired.
    #[tracing::instrument(skip(self, stream, limit, stale_chunks_notifier))]
    async fn reserve_chunks<'a>(
        &'a self,
        stream: ChunkStream<'a>,
        limit: usize,
        stale_chunks_notifier: &tokio::sync::mpsc::UnboundedSender<ChunkId>,
    ) -> Result<Vec<(Chunk, Submission)>, sqlx::Error> {
        stream
            .try_filter_map(|chunk| async move {
                let chunk_id = ChunkId::from((chunk.submission_id, chunk.chunk_index));
                let val = self
                    .reserver
                    .try_reserve(chunk_id, chunk_id, stale_chunks_notifier)
                    .map(|_| chunk);
                Ok(val)
            })
            .and_then(|chunk| async move {
                let conn = &mut self.pool.acquire().await?;
                let submission =
                    crate::common::submission::db::get_submission(chunk.submission_id, &mut **conn)
                        .await
                        .expect("get_submission while reserving failed");
                Ok((chunk, submission))
            })
            .take(limit)
            .try_collect()
            .await
    }

    #[tracing::instrument(skip(self, stale_chunks_notifier))]
    #[allow(clippy::type_complexity)]
    pub async fn fetch_and_reserve_chunks(
        &mut self,
        strategy: Strategy,
        limit: usize,
        stale_chunks_notifier: &tokio::sync::mpsc::UnboundedSender<ChunkId>,
    ) -> Result<Vec<(Chunk, Submission)>, E<DatabaseError, IncorrectUsage<LimitIsZero>>> {
        let start = tokio::time::Instant::now();

        if limit == 0 {
            return Err(E::R(IncorrectUsage(LimitIsZero())));
        }
        let mut conn = self.pool.acquire().await?;
        let stream = strategy.execute(&mut conn);
        let new_reservations = self
            .reserve_chunks(stream, limit, stale_chunks_notifier)
            .await?;

        self.reservations.lock().expect("No poison").extend(
            new_reservations.iter().map(|(chunk, _submission)| {
                ChunkId::from((chunk.submission_id, chunk.chunk_index))
            }),
        );

        histogram!(
            crate::prometheus::CONSUMER_FETCH_AND_RESERVE_CHUNKS_HISTOGRAM,
            &[
                ("limit", limit.to_string()),
                ("strategy", format!("{strategy:?}"))
            ]
        )
        .record(start.elapsed());
        Ok(new_reservations)
    }

    #[tracing::instrument(skip(self, output_content))]
    pub async fn complete_chunk(&mut self, id: ChunkId, output_content: chunk::Content) {
        // Only possible error indicates sender is closed, which means we're shutting down
        let _ = self
            .server_state
            .completer_tx
            .send(CompleterMessage::Complete {
                id,
                output_content,
                reservations: self.reservations.clone(),
            })
            .await;
        // let start = tokio::time::Instant::now();

        // let pool = self.pool.clone();
        // let reservations = self.reservations.clone();
        // let reserver = self.reserver.clone();
        // tokio::spawn(async move {
        //     let res: Result<(), anyhow::Error> = async move {
        //         let mut conn = pool.acquire().await?;
        //         chunk::db::complete_chunk(id, output_content, &mut conn).await?;
        //         reservations.lock().expect("No poison").remove(&id);
        //         // NOTE: Even in the unlikely event the query fails,
        //         // we want the chunk to be un-reserved
        //         if let Some(started_at) = reserver.finish_reservation(&id) { histogram!(crate::prometheus::CHUNKS_DURATION_COMPLETED_HISTOGRAM).record(started_at.elapsed()) }
        //         Ok(())
        //     }.await;

        //     if res.is_err() {
        //         tracing::error!("Failed to complete chunk: {:?}", res);
        //     }
        // });

        // histogram!(crate::prometheus::CONSUMER_COMPLETE_CHUNK_DURATION).record(start.elapsed());
        // Ok(())
    }

    pub async fn fail_chunk(&mut self, id: ChunkId, failure: String) {
        let _ = self
            .server_state
            .completer_tx
            .send(CompleterMessage::Fail {
                id,
                failure,
                reservations: self.reservations.clone(),
            })
            .await;

        // let start = tokio::time::Instant::now();

        // let pool = self.pool.clone();
        // let reservations = self.reservations.clone();
        // let reserver = self.reserver.clone();
        // tokio::spawn(async move {
        //     let res: Result<(), anyhow::Error> = async move {
        //         let mut conn = pool.acquire().await?;
        //         chunk::db::retry_or_fail_chunk(id, failure, &mut conn).await?;
        //         reservations.lock().expect("No poison").remove(&id);
        //         // NOTE: Even in the unlikely event the query fails,
        //         // we want the chunk to be un-reserved
        //         if let Some(started_at) = reserver.finish_reservation(&id) { histogram!(crate::prometheus::CHUNKS_DURATION_FAILED_HISTOGRAM).record(started_at.elapsed()) }
        //         Ok(())
        //     }.await;

        //     if res.is_err() {
        //         tracing::error!("Failed to fail chunk: {:?}", res);
        //     }
        // });

        // histogram!(crate::prometheus::CONSUMER_FAIL_CHUNK_DURATION).record(start.elapsed());
        // Ok(())
    }
}

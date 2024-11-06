use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use axum_prometheus::metrics::histogram;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::common::chunk;
use crate::common::errors::DatabaseError;
use crate::consumer::strategy::Strategy;
use crate::{
    common::{
        chunk::{Chunk, ChunkId},
        submission::Submission,
    },
    consumer::reserver::Reserver,
};

use super::ServerState;
use crate::common::errors::{
    E, IncorrectUsage, LimitIsZero,
};

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
}

impl Drop for ConsumerState {
    fn drop(&mut self) {

        let reservations = self.reservations.lock().unwrap();
        for reservation in &*reservations {
            self.reserver.finish_reservation(reservation)
        }
    }
}

impl ConsumerState {
    pub fn new(server_state: &ServerState) -> Self {
        Self {
            pool: server_state.pool.clone(),
            reserver: server_state.reserver.clone(),
            reservations: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Select chunks, and store them in the reserver, to make sure that re-running the same selection returns different results as long as they are reserved.
    ///
    /// - `query_fun` is a 'strategy' function returning a stream of chunks, something like `sqlx::query!("SELECT * FROM chunks WHERE ...").fetch(conn)`
    /// - `limit` is the maximum number of chunks to return. The query/stream will be evaluated until that many not-already-reserved chunks can be returned.
    ///    Of course, we'll return less if the stream is exhausted before `limit` is reached.
    /// - `stale_chunks_notifier` is a Tokio channel, which the reserver will automatically invoke when a particular chunk reservation has expired.
    #[tracing::instrument(skip(self, stream, limit, stale_chunks_notifier))]
    async fn reserve_chunks(
        &self,
        stream: impl Stream<Item = Result<ChunkId, sqlx::Error>>,
        limit: usize,
        stale_chunks_notifier: &tokio::sync::mpsc::UnboundedSender<ChunkId>,
    ) -> Result<Vec<(Chunk, Submission)>, sqlx::Error> {
        stream
            .try_filter_map(|chunk_id| async move {
                Ok(self
                    .reserver
                    .try_reserve(chunk_id, chunk_id, stale_chunks_notifier))
            })
            .and_then(|chunk_id| {
                async move {
                    let conn = &mut self.pool.acquire().await?;
                    let chunk = crate::common::chunk::db::get_chunk(chunk_id, &mut **conn).await?;
                    let submission = crate::common::submission::db::get_submission(chunk_id.submission_id, &mut **conn).await.expect("TODO: map error");
                    Ok((chunk, submission))
                }
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
            new_reservations
                .iter()
                .map(|(chunk, _submission)| ChunkId::from((chunk.submission_id, chunk.chunk_index))),
        );

        histogram!("consumer_fetch_and_reserve_chunks_duration", 
        &[("limit", limit.to_string()), ("strategy", format!("{strategy:?}"))]
    ).record(start.elapsed());
        Ok(new_reservations)
    }

    #[tracing::instrument(skip(self, output_content))]
    pub async fn complete_chunk(
        &mut self,
        id: ChunkId,
        output_content: chunk::Content,
    ) -> Result<(), DatabaseError> {
        let start = tokio::time::Instant::now();

        let pool = self.pool.clone();
        let reservations = self.reservations.clone();
        let reserver = self.reserver.clone();
        tokio::spawn(async move {
            let res: Result<(), anyhow::Error> = async move {
                let mut conn = pool.acquire().await?;
                chunk::db::complete_chunk(id, output_content, &mut conn).await?;
                reservations.lock().expect("No poison").remove(&id);
                // NOTE: Even in the unlikely event the query fails,
                // we want the chunk to be un-reserved
                reserver.finish_reservation(&id);
                Ok(())
            }.await;

            if res.is_err() {
                tracing::error!("Failed to fail chunk: {:?}", res);
            }
        });

        histogram!("consumer_complete_chunk_duration").record(start.elapsed());
        Ok(())
    }

    pub async fn fail_chunk(&mut self, id: ChunkId, failure: String) -> Result<(), DatabaseError> {
        let start = tokio::time::Instant::now();

        let pool = self.pool.clone();
        let reservations = self.reservations.clone();
        let reserver = self.reserver.clone();
        tokio::spawn(async move {
            let res: Result<(), anyhow::Error> = async move {
                let mut conn = pool.acquire().await?;
                chunk::db::retry_or_fail_chunk(id, failure, &mut conn).await?;
                reservations.lock().expect("No poison").remove(&id);
                // NOTE: Even in the unlikely event the query fails,
                // we want the chunk to be un-reserved
                reserver.finish_reservation(&id);
                Ok(())
            }.await;

            if res.is_err() {
                tracing::error!("Failed to fail chunk: {:?}", res);
            }
        });

        histogram!("consumer_fail_chunk_duration").record(start.elapsed());
        Ok(())
    }
}

use std::collections::HashSet;

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
    ChunkNotFound, E, IncorrectUsage, LimitIsZero, SubmissionNotFound,
};

// TODO: We currently clone the arc-like pool and reserver,
// but we could probably just give this struct a lifetime,
// as it will never outlive the ServerState it is created from.
#[derive(Debug)]
pub struct ConsumerState {
    // NOTE: This is an arc-like field, referring to the same DB pool.
    pool: sqlx::SqlitePool,
    // NOTE: This is an arc-like field, referring to the same reserver as all other ConsumerStates.
    reserver: Reserver<ChunkId, ChunkId>,
    // The following are the consumer-specific chunks that are currently reserved.
    reservations: HashSet<ChunkId>,
}

impl Drop for ConsumerState {
    fn drop(&mut self) {
        for reservation in &self.reservations {
            self.reserver.finish_reservation(reservation)
        }
    }
}

impl ConsumerState {
    pub fn new(server_state: &ServerState) -> Self {
        Self {
            pool: server_state.pool.clone(),
            reserver: server_state.reserver.clone(),
            reservations: HashSet::new(),
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
        stream: impl Stream<Item = Result<(Chunk, Submission), sqlx::Error>>,
        limit: usize,
        stale_chunks_notifier: &tokio::sync::mpsc::UnboundedSender<ChunkId>,
    ) -> Result<Vec<(Chunk, Submission)>, sqlx::Error> {
        stream
            .try_filter_map(|(chunk, submission)| async move {
                let chunk_id = (chunk.submission_id, chunk.chunk_index);
                Ok(self
                    .reserver
                    .try_reserve(chunk_id, chunk_id, stale_chunks_notifier)
                    .map(|_| (chunk, submission)))
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
        if limit == 0 {
            return Err(E::R(IncorrectUsage(LimitIsZero())));
        }
        let mut conn = self.pool.acquire().await?;
        let stream = strategy.execute(&mut conn);
        let new_reservations = self
            .reserve_chunks(stream, limit, stale_chunks_notifier)
            .await?;

        self.reservations.extend(
            new_reservations
                .iter()
                .map(|(chunk, _submission)| (chunk.submission_id, chunk.chunk_index)),
        );

        Ok(new_reservations)
    }

    #[tracing::instrument(skip(self, output_content))]
    pub async fn complete_chunk(
        &mut self,
        id: ChunkId,
        output_content: chunk::Content,
    ) -> Result<(), E<DatabaseError, E<SubmissionNotFound, ChunkNotFound>>> {
        let mut conn = self.pool.acquire().await?;
        // NOTE: Even in the unlikely event the query fails,
        // we want the chunk to be un-reserved
        let res = chunk::complete_chunk(id, output_content, &mut conn).await;
        self.reservations.remove(&id);
        self.reserver.finish_reservation(&id);
        res
    }

    pub async fn fail_chunk(&mut self, id: ChunkId, failure: String) -> Result<(), sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        // NOTE: Even in the unlikely event the query fails,
        // we want the chunk to be un-reserved
        let res = chunk::retry_or_fail_chunk(id, failure, &mut conn).await;
        self.reservations.remove(&id);
        self.reserver.finish_reservation(&id);
        res
    }
}

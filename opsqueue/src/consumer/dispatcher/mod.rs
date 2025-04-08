pub mod metastate;
pub mod reserver;

use crate::{
    common::{
        chunk::{Chunk, ChunkId},
        submission::Submission,
    },
    db::{magic::Bool, Connection, Pool, ReaderPool},
};
use futures::stream::{StreamExt as _, TryStreamExt as _};
use metastate::MetaState;
use reserver::Reserver;
use sqlx::QueryBuilder;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

use std::sync::Arc;

use super::strategy;
use crate::common::StrategicMetadataMap;

#[derive(Debug, Clone)]
pub struct Dispatcher {
    reserver: Reserver<ChunkId, ChunkId>,
    metastate: Arc<MetaState>,
}

impl Dispatcher {
    pub fn new(reservation_expiration: Duration) -> Self {
        let reserver = Reserver::new(reservation_expiration);
        let metastate = Arc::new(MetaState::default());

        Dispatcher {
            reserver,
            metastate,
        }
    }

    pub fn metastate(&self) -> &MetaState {
        &self.metastate
    }

    pub fn reserver(&self) -> &Reserver<ChunkId, ChunkId> {
        &self.reserver
    }

    fn insert_metadata(&self, metadata: &StrategicMetadataMap) {
        for (name, value) in metadata {
            self.metastate.increment(name, value);
        }
    }

    fn remove_metadata(&self, metadata: &StrategicMetadataMap) {
        for (name, value) in metadata {
            self.metastate.decrement(name, value);
        }
    }

    pub async fn fetch_and_reserve_chunks(
        &self,
        pool: &ReaderPool,
        strategy: strategy::Strategy,
        limit: usize,
        stale_chunks_notifier: &UnboundedSender<ChunkId>,
    ) -> Result<Vec<(Chunk, Submission)>, sqlx::Error> {
        let mut conn = pool.reader_conn().await?;
        let mut query_builder = QueryBuilder::new("");
        let stream = strategy
            .build_query(&mut query_builder, &self.metastate)
            .build_query_as()
            .fetch(conn.get_inner());
        stream
            .try_filter_map(|chunk| self.reserve_chunk(chunk, stale_chunks_notifier))
            .and_then(|chunk| self.join_chunk_with_submission_info(chunk, pool))
            .take(limit)
            .try_collect()
            .await
    }

    async fn reserve_chunk<E>(
        &self,
        chunk: Chunk,
        stale_chunks_notifier: &UnboundedSender<ChunkId>,
    ) -> Result<Option<Chunk>, E> {
        let chunk_id = ChunkId::from((chunk.submission_id, chunk.chunk_index));
        let val = self
            .reserver
            .try_reserve(chunk_id, chunk_id, stale_chunks_notifier)
            .map(|_| chunk);
        Ok(val)
    }

    async fn join_chunk_with_submission_info(
        &self,
        chunk: Chunk,
        pool: &Pool<impl Bool>,
    ) -> Result<(Chunk, Submission), sqlx::Error> {
        let mut conn = pool.acquire().await?;
        let submission =
            crate::common::submission::db::get_submission(chunk.submission_id, &mut conn)
                .await
                .expect("get_submission while reserving failed");
        let metadata = crate::common::submission::db::get_submission_strategic_metadata(
            chunk.submission_id,
            &mut conn,
        )
        .await
        .expect("get_submission_strategic_metadata while reserving failed");
        self.insert_metadata(&metadata);
        Ok((chunk, submission))
    }

    pub fn finish_reservations_sync<'a>(&self, reservations: impl Iterator<Item = &'a ChunkId>) {
        for reservation in reservations {
            let _ = self.reserver.finish_reservation_sync(reservation);
        }
    }

    pub async fn finish_reservation(
        &self,
        conn: impl Connection,
        id: ChunkId,
        delayed: bool,
    ) -> Option<Instant> {
        let maybe_started_at = self.reserver.finish_reservation(&id, delayed).await;

        // In the highly unlikely event that this DB query fails,
        // we still want to continue
        let metadata = crate::common::submission::db::get_submission_strategic_metadata(
            id.submission_id,
            conn,
        )
        .await
        .unwrap_or_default();

        self.remove_metadata(&metadata);
        maybe_started_at
    }

    pub fn run_pending_tasks_periodically(&self, cancellation_token: CancellationToken) {
        self.reserver
            .run_pending_tasks_periodically(cancellation_token);
    }
}

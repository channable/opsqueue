use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use axum_prometheus::metrics::histogram;
use opentelemetry::trace::TraceContextExt;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::common::chunk;
use crate::common::errors::DatabaseError;
use crate::common::{
    chunk::{Chunk, ChunkId},
    submission::Submission,
};
use crate::consumer::strategy;

use super::CompleterMessage;
use super::ServerState;
use crate::common::errors::{E, IncorrectUsage, LimitIsZero};

#[derive(Debug, Clone)]
pub struct ConsumerState {
    server_state: Arc<ServerState>,
    // The following are the consumer-specific chunks that are currently reserved.
    reservations: Arc<Mutex<HashSet<ChunkId>>>,
}

impl Drop for ConsumerState {
    fn drop(&mut self) {
        let reservations = self.reservations.lock().unwrap();

        // We're not tracking chunk durations that are unreserved during consumer shutdown,
        // as those will be by definition unfinished
        let had_reservations = !reservations.is_empty();
        self.server_state
            .dispatcher
            .finish_reservations_sync(reservations.iter());
        if had_reservations {
            self.server_state.notify_on_insert.notify_waiters();
        }
    }
}

impl ConsumerState {
    #[must_use]
    pub fn new(server_state: &Arc<ServerState>) -> Self {
        Self {
            reservations: Arc::new(Mutex::new(HashSet::new())),
            server_state: server_state.clone(),
        }
    }

    #[tracing::instrument(skip(self, stale_chunks_notifier))]
    #[allow(clippy::type_complexity)]
    /// Fetch and reserve chunks for this connection state.
    ///
    /// # Panics
    ///
    /// Panics if the reservations mutex is poisoned.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid `limit` values or underlying database failures.
    pub async fn fetch_and_reserve_chunks(
        &mut self,
        strategy: strategy::Strategy,
        limit: usize,
        stale_chunks_notifier: &tokio::sync::mpsc::UnboundedSender<ChunkId>,
    ) -> Result<Vec<(Chunk, Submission)>, E<DatabaseError, IncorrectUsage<LimitIsZero>>> {
        let start = tokio::time::Instant::now();
        if limit == 0 {
            return Err(E::R(IncorrectUsage(LimitIsZero())));
        }

        let new_reservations = self
            .server_state
            .dispatcher
            .fetch_and_reserve_chunks(
                self.server_state.pool.reader_pool(),
                strategy.clone(),
                limit,
                stale_chunks_notifier,
            )
            .await?;

        self.reservations.lock().expect("No poison").extend(
            new_reservations.iter().map(|(chunk, _submission)| {
                ChunkId::from((chunk.submission_id, chunk.chunk_index))
            }),
        );

        // Link the consumer's trace with the submission's existing trace context
        let reservation_span = tracing::info_span!("fetch_and_reserve_chunks-reserved");

        if new_reservations.len() == 1 {
            let submission = &new_reservations[0].1;
            let context = crate::tracing::json_to_context(&submission.otel_trace_carrier);
            let _ = reservation_span.set_parent(context);
        } else {
            for (_, submission) in &new_reservations {
                let context = crate::tracing::json_to_context(&submission.otel_trace_carrier);
                reservation_span.add_link(context.span().span_context().clone());
            }
        }

        let _ = reservation_span.enter();

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
        self.enqueue_chunk_outcome(id, CompleterMessage::Complete { id, output_content })
            .await;
    }

    pub async fn fail_chunk(&mut self, id: ChunkId, failure: String) {
        self.enqueue_chunk_outcome(id, CompleterMessage::Fail { id, failure })
            .await;
    }

    async fn enqueue_chunk_outcome(&mut self, id: ChunkId, outcome: CompleterMessage) {
        // Once queued, the completer owns the reservation even if this connection closes.
        if let Ok(()) = self.server_state.completer_tx.send(outcome).await {
            self.reservations.lock().expect("No poison").remove(&id);
        } else {
            tracing::debug!(?id, "Chunk completer unavailable while shutting down");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::{Notify, broadcast, mpsc};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::common::StrategicMetadataMap;
    use crate::common::chunk::ChunkSize;
    use crate::common::submission::InitialSubmissionStatus;
    use crate::db::DBPools;

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn queued_completion_stays_reserved_when_consumer_disconnects(pool: sqlx::SqlitePool) {
        let db_pools = DBPools::from_test_pool(&pool);
        let mut conn = db_pools.writer_conn().await.unwrap();
        crate::common::submission::db::insert_submission_from_chunks(
            None,
            vec![Some("work".into())],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            InitialSubmissionStatus::default(),
            &mut conn,
        )
        .await
        .unwrap();
        drop(conn);

        let notify_on_insert = Arc::new(Notify::new());
        let server_state = Arc::new(ServerState::new(
            db_pools,
            notify_on_insert.clone(),
            broadcast::channel(10).0,
            CancellationToken::new(),
            Duration::from_mins(1),
            Box::leak(Box::default()),
        ));
        let mut consumer = ConsumerState::new(&server_state);
        let (tx, _rx) = mpsc::unbounded_channel();
        let chunks = consumer
            .fetch_and_reserve_chunks(strategy::Strategy::Oldest, 1, &tx)
            .await
            .unwrap();
        let chunk_id = ChunkId::from((chunks[0].0.submission_id, chunks[0].0.chunk_index));

        let mut notification = Box::pin(notify_on_insert.notified_owned());
        notification.as_mut().enable();
        consumer.complete_chunk(chunk_id, None).await;
        drop(consumer);

        assert!(server_state.dispatcher.reserver().is_reserved(&chunk_id));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), notification)
                .await
                .is_err(),
            "A queued completion should not wake waiting consumers on disconnect"
        );
    }
}

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use axum_prometheus::metrics::histogram;
use opentelemetry::trace::TraceContextExt;
use tracing::Span;
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
use crate::common::errors::{IncorrectUsage, LimitIsZero, E};

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
        self.server_state
            .dispatcher
            .finish_reservations_sync(reservations.iter());
    }
}

impl ConsumerState {
    pub fn new(server_state: &Arc<ServerState>) -> Self {
        Self {
            // pool: server_state.pool.read_pool.clone(),
            // dispatcher: server_state.dispatcher.clone(),
            reservations: Arc::new(Mutex::new(HashSet::new())),
            server_state: server_state.clone(),
        }
    }

    #[tracing::instrument(skip(self, stale_chunks_notifier))]
    #[allow(clippy::type_complexity)]
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
        if new_reservations.len() == 1 {
            let submission = &new_reservations[0].1;
            let context = crate::tracing::json_to_context(&submission.otel_trace_carrier);
            Span::current().set_parent(context);
        } else {
            for (_, submission) in &new_reservations {
                let context = crate::tracing::json_to_context(&submission.otel_trace_carrier);
                Span::current().add_link(context.span().span_context().clone());
            }
        }

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
    }
}

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    extract::{State, WebSocketUpgrade},
    routing::get,
    Router,
};
use axum_prometheus::metrics::{gauge, histogram};
use tokio::{select, sync::Notify};
use tokio_util::sync::CancellationToken;

use crate::{
    common::chunk::ChunkId,
    config::Config,
    db::{self, DBPools},
};

use super::dispatcher::Dispatcher;

pub mod conn;
pub mod state;

pub async fn serve_for_tests(
    pool: DBPools,
    server_addr: Box<str>,
    cancellation_token: CancellationToken,
    reservation_expiration: Duration,
) {
    let notify_on_insert = Arc::new(Notify::new());
    let config = Box::leak(Box::default());
    let state = ServerState::new(
        pool,
        notify_on_insert,
        cancellation_token.clone(),
        reservation_expiration,
        config,
    );
    let router = ServerState::build_router(state);
    let app = Router::new().nest("/consumer", router);
    let listener = tokio::net::TcpListener::bind(&*server_addr)
        .await
        .expect("Failed to bind to consumer server address");

    tracing::info!("Consumer WebSocket server listening at {server_addr}...");
    select! {
      _ = cancellation_token.cancelled() => {},
      res = axum::serve(listener, app) => res.expect("Failed to start consumer server"),
    }
}

#[derive(Debug)]
pub struct ServerState {
    pool: DBPools,
    dispatcher: Dispatcher,
    completer: Option<Completer>,
    completer_tx: tokio::sync::mpsc::Sender<CompleterMessage>,
    notify_on_insert: Arc<Notify>,
    cancellation_token: CancellationToken,
    config: &'static Config,
}

impl ServerState {
    pub fn new(
        pool: DBPools,
        notify_on_insert: Arc<Notify>,
        cancellation_token: CancellationToken,
        reservation_expiration: Duration,
        config: &'static Config,
    ) -> Self {
        let dispatcher = Dispatcher::new(reservation_expiration);
        let (completer, completer_tx) = Completer::new(pool.writer_pool(), &dispatcher);
        Self {
            pool,
            completer: Some(completer),
            completer_tx,
            notify_on_insert,
            cancellation_token,
            dispatcher,
            config,
        }
    }

    pub fn run_background(mut self) -> Self {
        self.dispatcher
            .run_pending_tasks_periodically(self.cancellation_token.clone());
        let completer = std::mem::take(&mut self.completer)
            .expect("Error: Completer not available. Was `run_background` called twice?");
        completer.run(self.cancellation_token.clone());
        self
    }

    pub fn build_router(self: ServerState) -> Router<()> {
        Router::new()
            .route("/", get(ws_accept_handler))
            .with_state(Arc::new(self))
    }
}

async fn ws_accept_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
) -> axum::response::Response {
    ws.on_upgrade(|ws_stream| async move {
        gauge!(crate::prometheus::CONSUMERS_CONNECTED_GAUGE).increment(1);

        let res = conn::ConsumerConn::new(&state, ws_stream).run().await;
        match res {
            Ok(()) => {}
            Err(e) if e.is_internal_error() => {
                tracing::error!(
                    "Closed websocket connection because of internal error, details: {e:?}"
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Closed websocket connection because of client error, details: {e:?}"
                );
            }
        }

        gauge!(crate::prometheus::CONSUMERS_CONNECTED_GAUGE).decrement(1);
    })
}

pub enum CompleterMessage {
    Complete {
        id: ChunkId,
        output_content: crate::common::chunk::Content,
        reservations: Arc<Mutex<HashSet<ChunkId>>>,
    },
    Fail {
        id: ChunkId,
        failure: String,
        reservations: Arc<Mutex<HashSet<ChunkId>>>,
    },
}

impl std::fmt::Debug for CompleterMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Complete {
                id,
                output_content,
                reservations: _,
            } => f
                .debug_struct("Complete")
                .field("id", id)
                .field("output_content", output_content)
                .finish_non_exhaustive(),
            Self::Fail {
                id,
                failure,
                reservations: _,
            } => f
                .debug_struct("Fail")
                .field("id", id)
                .field("failure", failure)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Debug)]
pub struct Completer {
    mailbox: tokio::sync::mpsc::Receiver<CompleterMessage>,
    pool: db::WriterPool,
    dispatcher: Dispatcher,
    count: usize,
}

impl Completer {
    pub fn new(
        pool: &db::WriterPool,
        dispatcher: &Dispatcher,
    ) -> (Self, tokio::sync::mpsc::Sender<CompleterMessage>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let pool = pool.clone();
        let me = Self {
            mailbox: rx,
            pool: pool.clone(),
            dispatcher: dispatcher.clone(),
            count: 0,
        };
        (me, tx)
    }
    pub fn run(mut self, cancellation_token: CancellationToken) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = cancellation_token.cancelled() => break,
                    Some(msg) = self.mailbox.recv() => self.handle_message(msg).await,
                }
                // Log some indication of progress every so often:
                self.count = self.count.saturating_add(1);
                if self.count % 1000 == 0 {
                    tracing::info!("Processed {} chunks", self.count);
                }
            }
        });
    }

    #[tracing::instrument(name = "Completer::handle_message", level = "info", skip(self))]
    async fn handle_message(&mut self, msg: CompleterMessage) {
        let start = tokio::time::Instant::now();

        let res: anyhow::Result<()> = async move {
            let mut conn = self.pool.writer_conn().await?;

            match msg {
                CompleterMessage::Complete {
                    id,
                    output_content,
                    reservations,
                } => {
                    // Even in the unlikely event that the DB write fails,
                    // we still want to unreserve the chunk
                    let db_res =
                        crate::common::chunk::db::complete_chunk(id, output_content, &mut conn)
                            .await;

                    reservations.lock().expect("No poison").remove(&id);
                    if let Some(started_at) = self
                        .dispatcher
                        .finish_reservation(&mut conn, id, true)
                        .await
                    {
                        histogram!(crate::prometheus::CHUNKS_DURATION_COMPLETED_HISTOGRAM)
                            .record(started_at.elapsed())
                    }
                    histogram!(crate::prometheus::CONSUMER_COMPLETE_CHUNK_DURATION)
                        .record(start.elapsed());
                    db_res?;
                    Ok(())
                }
                CompleterMessage::Fail {
                    id,
                    failure,
                    reservations,
                } => {
                    // Even in the unlikely event that the DB write fails,
                    // we still want to unreserve the chunk
                    let failed_permanently =
                        crate::common::chunk::db::retry_or_fail_chunk(id, failure, &mut conn).await;
                    reservations.lock().expect("No poison").remove(&id);
                    let maybe_started_at = self
                        .dispatcher
                        .finish_reservation(
                            &mut conn,
                            id,
                            *failed_permanently.as_ref().unwrap_or(&false),
                        )
                        .await;
                    if let Some(started_at) = maybe_started_at {
                        histogram!(crate::prometheus::CHUNKS_DURATION_FAILED_HISTOGRAM)
                            .record(started_at.elapsed())
                    }

                    histogram!(crate::prometheus::CONSUMER_FAIL_CHUNK_DURATION)
                        .record(start.elapsed());

                    failed_permanently?;
                    Ok(())
                }
            }
        }
        .await;
        match res {
            Ok(()) => {}
            Err(err) => tracing::error!("Error in chunk completer: {err}"),
        }
    }
}

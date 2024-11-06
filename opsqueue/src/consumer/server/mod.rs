use std::{sync::Arc, time::Duration};

use axum::{
    extract::{State, WebSocketUpgrade},
    routing::get,
    Router,
};
use metrics::{counter, gauge};
use tokio::{select, sync::Notify};
use tokio_util::sync::CancellationToken;

use crate::common::chunk::ChunkId;

use super::reserver::Reserver;

pub mod conn;
pub mod state;

pub async fn serve_for_tests(
    pool: sqlx::SqlitePool,
    server_addr: Box<str>,
    cancellation_token: CancellationToken,
    reservation_expiration: Duration,
) {
    let notify_on_insert = Arc::new(Notify::new());
    let state = ServerState::new(pool, notify_on_insert, cancellation_token.clone(), reservation_expiration);
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

#[derive(Debug, Clone)]
pub struct ServerState {
    pool: sqlx::SqlitePool,
    notify_on_insert: Arc<Notify>,
    cancellation_token: CancellationToken,
    reserver: Reserver<ChunkId, ChunkId>,
}

impl ServerState {
    pub fn new(
        pool: sqlx::SqlitePool,
        notify_on_insert: Arc<Notify>,
        cancellation_token: CancellationToken,
        reservation_expiration: Duration,
    ) -> Self {
        let reserver = Reserver::new(reservation_expiration);
        Self {
            pool,
            notify_on_insert,
            cancellation_token,
            reserver,
        }
    }

    pub fn build_router(self: ServerState) -> Router<()> {
        Router::new()
            .route("/", get(ws_accept_handler))
            .with_state(self)
    }
}

async fn ws_accept_handler(
    ws: WebSocketUpgrade,
    State(state): State<ServerState>,
) -> axum::response::Response {
    ws.on_upgrade(|ws_stream| async move {
        gauge!("consumers_connected").increment(1);

        let res = 
            conn::ConsumerConn::new(&state, ws_stream)
            .run()
            .await;

        tracing::warn!("Closed websocket connection, reason: {:?}", &res);
        gauge!("consumers_connected").decrement(1);
    })
}

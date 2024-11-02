pub mod common;
pub mod consumer;
pub mod producer;

#[cfg(feature = "client-logic")]
pub mod object_store;
use std::time::Duration;

#[cfg(feature = "server-logic")]
use std::sync::{atomic::AtomicBool, Arc};

#[cfg(feature = "server-logic")]
use axum::{routing::get, Router};
use backon::{BackoffBuilder, FibonacciBuilder};
#[cfg(feature = "server-logic")]
use http::StatusCode;

#[cfg(feature = "server-logic")]
use sqlx::{
    migrate::MigrateDatabase,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
    Connection, Sqlite, SqliteConnection, SqlitePool,
};
#[cfg(feature = "server-logic")]
use tokio::select;
#[cfg(feature = "server-logic")]
use tokio_util::sync::CancellationToken;

// TOOD: Set max retries to `None`;
// will require either writing our own Backoff (iterator)
// or extending the backon crate.
fn retry_policy() -> impl BackoffBuilder {
    FibonacciBuilder::default()
    .with_jitter()
    .with_min_delay(Duration::from_millis(10))
    .with_max_delay(Duration::from_secs(10))
    .with_max_times(usize::MAX)
}


#[cfg(feature = "server-logic")]
pub async fn serve_producer_and_consumer(
    server_addr: &str,
    pool: &SqlitePool,
    reservation_expiration: Duration,
    cancellation_token: &CancellationToken,
    app_healthy_flag: &Arc<AtomicBool>,
) -> Result<(), std::io::Error> {
    use backon::Retryable;

    (|| async {
    let router = build_router(
        pool.clone(),
        reservation_expiration,
        cancellation_token.clone(),
        app_healthy_flag.clone(),
    );
    let listener = tokio::net::TcpListener::bind(server_addr)
        .await?;

    axum::serve(listener, router)
        .with_graceful_shutdown(cancellation_token.clone().cancelled_owned())
        .await?;
    Ok(())
    }).retry(retry_policy()).notify(|e, d| {
        tracing::error!("Error when binding server address: {e:?}, retrying in {d:?}")
    }).await
}

#[cfg(feature = "server-logic")]
pub fn build_router(
    pool: SqlitePool,
    reservation_expiration: Duration,
    cancellation_token: CancellationToken,
    app_healthy_flag: Arc<AtomicBool>,
) -> Router<()> {
    let consumer_routes = consumer::server::ServerState::new(
        pool.clone(),
        cancellation_token.clone(),
        reservation_expiration,
    )
    .build_router();
    let producer_routes = producer::server::ServerState::new(pool).build_router();
    let routes = Router::new()
        .route("/ping", get(|| async move { ping(app_healthy_flag).await }))
        .nest("/producer", producer_routes)
        .nest("/consumer", consumer_routes);

    let tracing_middleware = tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(tower_http::trace::DefaultMakeSpan::new())
        .on_request(tower_http::trace::DefaultOnRequest::new())
        .on_response(tower_http::trace::DefaultOnResponse::new().level(tracing::Level::INFO));

    routes.layer(tracing_middleware)
}

/// Used as a very simple health check by consul.
#[cfg(feature = "server-logic")]
async fn ping(app_heatlhy_flag: Arc<AtomicBool>) -> (StatusCode, &'static str) {
    async {
        if app_heatlhy_flag.load(std::sync::atomic::Ordering::Relaxed) {
            (StatusCode::OK, "pong")
        } else {
            (StatusCode::SERVICE_UNAVAILABLE, "unhealthy")
        }
    }
    .await
}

#[cfg(feature = "server-logic")]
pub async fn app_watchdog(
    app_healthy_flag: Arc<AtomicBool>,
    pool: &SqlitePool,
    cancellation_token: CancellationToken,
) {
    loop {
        // For now this is just a single check, but in the future
        // we might have many checks; we first gather them and then write to the atomic bool once.
        let is_app_healthy = is_db_healthy(pool).await;
        app_healthy_flag.store(is_app_healthy, std::sync::atomic::Ordering::Relaxed);

        select! {
            () = cancellation_token.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
        }
    }
    // Set to unhealthy when shutting down
    app_healthy_flag.store(false, std::sync::atomic::Ordering::Relaxed);
}

/// We check whether we can not only reach the DB but especially if we can run a transaction.
///
/// This handles the case where for whatever reason some other thing
/// holds the write lock for the DB for a (too) long time.
#[cfg(feature = "server-logic")]
async fn is_db_healthy(pool: &SqlitePool) -> bool {
    async move {
        let mut tx = pool.begin().await?;
        let _count = common::submission::count_submissions(&mut *tx).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await
    .is_ok()
}

#[cfg(feature = "server-logic")]
pub fn db_options(database_filename: &str) -> SqliteConnectOptions {
    SqliteConnectOptions::new()
        .filename(database_filename)
        .journal_mode(SqliteJournalMode::Wal) // This is the default for Sqlx-sqlite and also for Litestream, but good to make sure it is set
        .synchronous(SqliteSynchronous::Normal) // Full is not needed because we use WAL mode
}

#[cfg(feature = "server-logic")]
pub async fn db_connect_pool(database_filename: &str) -> SqlitePool {
    SqlitePool::connect_with(db_options(database_filename))
        .await
        .expect("Could not connect to sqlite DB")
}

#[cfg(feature = "server-logic")]
pub async fn db_connect_single(database_filename: &str) -> SqliteConnection {
    SqliteConnection::connect_with(&db_options(database_filename))
        .await
        .expect("Could not connect to sqlite DB")
}

#[cfg(feature = "server-logic")]
pub async fn ensure_db_exists(database_filename: &str) {
    if !Sqlite::database_exists(database_filename)
        .await
        .unwrap_or(false)
    {
        tracing::info!("Creating backing sqlite DB {}", database_filename);
        Sqlite::create_database(database_filename)
            .await
            .expect("Could not create backing sqlite DB");
        tracing::info!("Finished creating backing sqlite DB {}", database_filename);
    } else {
        tracing::info!("Starting up using existing sqlite DB {}", database_filename);
    }
}

#[cfg(feature = "server-logic")]
pub async fn ensure_db_migrated(db: &SqlitePool) {
    tracing::info!("Migrating backing DB");
    sqlx::migrate!("./migrations")
        .run(db)
        .await
        .expect("DB migrations failed");
    tracing::info!("Finished migrating backing DB");
}

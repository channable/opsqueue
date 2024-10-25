use std::{sync::{atomic::AtomicBool, Arc}, time::Duration};

use axum::{routing::get, Router};
use http::StatusCode;
use sqlx::{
    migrate::MigrateDatabase,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
    Connection, Sqlite, SqliteConnection, SqlitePool,
};
use tokio_util::sync::CancellationToken;
use tokio::select;

pub mod common;
pub mod consumer;
pub mod producer;
pub mod object_store;

pub async fn serve_producer_and_consumer(server_addr: &str, pool: SqlitePool, reservation_expiration: Duration, cancellation_token: CancellationToken, app_healthy_flag: Arc<AtomicBool>) {
    let router = build_router(pool, reservation_expiration, cancellation_token.clone(), app_healthy_flag);
    let listener = tokio::net::TcpListener::bind(server_addr).await.expect("Failed to bind to web server address");

    let res = axum::serve(listener, router).with_graceful_shutdown(cancellation_token.cancelled_owned()).await;
    res.expect("Failed to start web server")
}

pub fn build_router(pool: SqlitePool, reservation_expiration: Duration, cancellation_token: CancellationToken, app_healthy_flag: Arc<AtomicBool>) -> Router<()>{
    let consumer_routes = consumer::server::ServerState::new(pool.clone(), cancellation_token.clone(), reservation_expiration).build_router();
    let producer_routes = producer::server::ServerState::new(pool).build_router();
    let routes =
        Router::new()
        .route("/ping", get(|| async move { ping(app_healthy_flag).await }))
        .nest("/producer", producer_routes)
        .nest("/consumer", consumer_routes);

    let tracing_middleware =
        tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(tower_http::trace::DefaultMakeSpan::new() )
        .on_request(tower_http::trace::DefaultOnRequest::new()
        )
        .on_response(tower_http::trace::DefaultOnResponse::new()
            .level(tracing::Level::INFO));

    routes.layer(tracing_middleware)
}


/// Used as a very simple health check by consul.
async fn ping(app_heatlhy_flag: Arc<AtomicBool>) -> (StatusCode, &'static str) {
    async {
        if app_heatlhy_flag.load(std::sync::atomic::Ordering::Relaxed) {
            (StatusCode::OK, "pong")
        } else {
            (StatusCode::SERVICE_UNAVAILABLE, "unhealthy")
        }
    }.await
}

pub async fn app_watchdog(app_healthy_flag: Arc<AtomicBool>, pool: SqlitePool, cancellation_token: CancellationToken) {
    loop {
        // For now this is just a single check, but in the future
        // we might have many checks; we first gather them and then write to the atomic bool once.
        let is_app_healthy = is_db_healthy(&pool).await;
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
async fn is_db_healthy(pool: &SqlitePool) -> bool {
    async move {
        let mut tx = pool.begin().await?;
        let _count = common::submission::count_submissions(&mut *tx).await?;
        Ok::<_, anyhow::Error>(())
    }.await.is_ok()
}


pub fn db_options(database_filename: &str) -> SqliteConnectOptions {
    SqliteConnectOptions::new()
        .filename(database_filename)
        .journal_mode(SqliteJournalMode::Wal) // This is the default for Sqlx-sqlite and also for Litestream, but good to make sure it is set
        .synchronous(SqliteSynchronous::Normal) // Full is not needed because we use WAL mode
}

pub async fn db_connect_pool(database_filename: &str) -> SqlitePool {
    SqlitePool::connect_with(db_options(database_filename))
        .await
        .expect("Could not connect to sqlite DB")
}

pub async fn db_connect_single(database_filename: &str) -> SqliteConnection {
    SqliteConnection::connect_with(&db_options(database_filename))
        .await
        .expect("Could not connect to sqlite DB")
}

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

pub async fn ensure_db_migrated(db: &SqlitePool) {
    tracing::info!("Migrating backing DB");
    sqlx::migrate!("./migrations")
        .run(db)
        .await
        .expect("DB migrations failed");
    tracing::info!("Finished migrating backing DB");
}

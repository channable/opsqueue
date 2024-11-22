use std::{
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use axum::{routing::get, Router};
use backon::{BackoffBuilder, FibonacciBuilder};
use http::StatusCode;

use crate::db::DBPools;
use tokio::select;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

fn retry_policy() -> impl BackoffBuilder {
    FibonacciBuilder::default()
        .with_jitter()
        .with_min_delay(Duration::from_millis(10))
        .with_max_delay(Duration::from_secs(10))
        .without_max_times()
}

#[cfg(feature = "server-logic")]
pub async fn serve_producer_and_consumer(
    config: &Arc<crate::config::Config>,
    server_addr: &str,
    pool: &DBPools,
    reservation_expiration: Duration,
    cancellation_token: &CancellationToken,
    app_healthy_flag: &Arc<AtomicBool>,
    prometheus_config: crate::prometheus::PrometheusConfig,
) -> Result<(), std::io::Error> {
    use backon::Retryable;

    (|| async {
        let router = build_router(
            config,
            pool.clone(),
            reservation_expiration,
            cancellation_token.clone(),
            app_healthy_flag.clone(),
            prometheus_config.clone(),
        );
        let listener = tokio::net::TcpListener::bind(server_addr).await?;

        axum::serve(listener, router)
            .with_graceful_shutdown(cancellation_token.clone().cancelled_owned())
            .await?;
        Ok(())
    })
    .retry(retry_policy())
    .notify(|e, d| tracing::error!("Error when binding server address: {e:?}, retrying in {d:?}"))
    .await
}

#[cfg(feature = "server-logic")]
pub fn build_router(
    config: &Arc<crate::config::Config>,
    pool: DBPools,
    reservation_expiration: Duration,
    cancellation_token: CancellationToken,
    app_healthy_flag: Arc<AtomicBool>,
    prometheus_config: crate::prometheus::PrometheusConfig,
) -> Router<()> {
    let notify_on_insert = Arc::new(Notify::new());

    let consumer_routes = crate::consumer::server::ServerState::new(
        pool.clone(),
        notify_on_insert.clone(),
        cancellation_token.clone(),
        reservation_expiration,
        config,
    )
    .run_background()
    .build_router();
    let producer_routes =
        crate::producer::server::ServerState::new(pool, notify_on_insert).build_router();

    let routes = Router::new()
        .nest("/producer", producer_routes)
        .nest("/consumer", consumer_routes);

    // NOTE: For the initial release, these values make sense for extra introspection.
    // In some future version, we probably want to lower these log levels down to DEBUG
    // and stop logging a pair of lines for every HTTP request.
    let tracing_middleware = tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(tracing::Level::INFO))
        .on_request(tower_http::trace::DefaultOnRequest::new())
        .on_response(tower_http::trace::DefaultOnResponse::new().level(tracing::Level::INFO));

    let traced_routes = routes.layer(tracing_middleware).layer(prometheus_config.0);

    // We do not want to trace, log nor gather metrics for the `ping` or `metrics` endpoints
    traced_routes
        .route("/ping", get(|| async move { ping(app_healthy_flag).await }))
        .route(
            "/metrics",
            get(|| async move { prometheus_config.1.render() }),
        )
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
    pool: &DBPools,
    cancellation_token: CancellationToken,
) {
    loop {
        // For now this is just a single check, but in the future
        // we might have many checks; we first gather them and then write to the atomic bool once.
        let is_app_healthy = crate::db::is_db_healthy(&pool.write_pool).await;
        app_healthy_flag.store(is_app_healthy, std::sync::atomic::Ordering::Relaxed);

        select! {
            () = cancellation_token.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(10)) => {},
        }
    }
    // Set to unhealthy when shutting down
    app_healthy_flag.store(false, std::sync::atomic::Ordering::Relaxed);
}

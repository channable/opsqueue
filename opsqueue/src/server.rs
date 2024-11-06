use std::{
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use axum::{routing::get, Router};
use axum_prometheus::PrometheusMetricLayer;
use backon::{BackoffBuilder, FibonacciBuilder};
use http::StatusCode;

use sqlx::sqlite::SqlitePool;

use tokio::select;
use tokio_util::sync::CancellationToken;

/// Taken from axum_prometheus::utils::SECONDS_DURATION_BUCKETS
/// but including two more buckets at the lower end
pub const SECONDS_DURATION_BUCKETS: &[f64; 13] = &[0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

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
    use axum_prometheus::{metrics_exporter_prometheus::{Matcher, PrometheusBuilder}, utils::SECONDS_DURATION_BUCKETS, PrometheusMetricLayerBuilder, AXUM_HTTP_REQUESTS_DURATION_SECONDS};
    use tokio::sync::Notify;
    let notify_on_insert = Arc::new(Notify::new());

    let consumer_routes = crate::consumer::server::ServerState::new(
        pool.clone(),
        notify_on_insert.clone(),
        cancellation_token.clone(),
        reservation_expiration,
    )
    .build_router();
    let producer_routes = crate::producer::server::ServerState::new(pool, notify_on_insert).build_router();

    let (prometheus_middleware, prometheus_metrics_handle) = // PrometheusMetricLayer::pair();
      PrometheusMetricLayerBuilder::new()
      .with_prefix("opsqueue")
      .with_ignore_patterns(&["/metrics"])
      .enable_response_body_size(true)
      // .with_default_metrics()
      .with_metrics_from_fn(|| {
            PrometheusBuilder::default()
                .set_buckets_for_metric(
                    Matcher::Full(AXUM_HTTP_REQUESTS_DURATION_SECONDS.to_string()),
                    SECONDS_DURATION_BUCKETS,
                )
                .unwrap()
                .install_recorder()
                .unwrap()
        })
      .build_pair();

    let routes = Router::new()
        .nest("/producer", producer_routes)
        .nest("/consumer", consumer_routes)
        .route("/ping", get(|| async move { ping(app_healthy_flag).await }))
        .route("/metrics", get(|| async move {prometheus_metrics_handle.render() }));

    let tracing_middleware = tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(tower_http::trace::DefaultMakeSpan::new())
        .on_request(tower_http::trace::DefaultOnRequest::new())
        .on_response(tower_http::trace::DefaultOnResponse::new().level(tracing::Level::INFO));

    routes
    .layer(prometheus_middleware)
    .layer(tracing_middleware)
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
        let is_app_healthy = crate::db::is_db_healthy(pool).await;
        app_healthy_flag.store(is_app_healthy, std::sync::atomic::Ordering::Relaxed);

        select! {
            () = cancellation_token.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(10)) => {},
        }
    }
    // Set to unhealthy when shutting down
    app_healthy_flag.store(false, std::sync::atomic::Ordering::Relaxed);
}


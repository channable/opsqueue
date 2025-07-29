//! Defines the HTTP endpoints that are used by both the `producer` and `consumer` APIs
use std::{
    any::Any,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use axum::{routing::get, Router};
use backon::{BackoffBuilder, FibonacciBuilder};
use http::{header, Response, StatusCode};

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
    config: &'static crate::config::Config,
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
    config: &'static crate::config::Config,
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
        .on_request(|request: &http::Request<_>, span: &tracing::Span| {
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            span.set_parent(crate::tracing::context_from_headers(request.headers()));
        })
        .on_response(tower_http::trace::DefaultOnResponse::new().level(tracing::Level::INFO));

    let traced_routes = routes.layer(tracing_middleware).layer(prometheus_config.0);

    // We do not want to trace, log nor gather metrics for the `ping` or `metrics` endpoints
    let routes = traced_routes
        .route("/ping", get(|| async move { ping(app_healthy_flag).await }))
        .route("/version", get(version_endpoint))
        .route(
            "/metrics",
            get(|| async move { prometheus_config.1.render() }),
        )
        .route(
            "/intentionally-panic-for-tests",
            get(intentionally_panic_for_tests),
        );

    routes.layer(tower_http::catch_panic::CatchPanicLayer::custom(
        handle_panic,
    ))
}

async fn intentionally_panic_for_tests() {
    panic!("Boom! A big explosion! This allows us to test the panic handler + trace/sentry integration")
}

pub async fn version_endpoint() -> String {
    crate::version_info()
}

fn handle_panic(err: Box<dyn Any + Send + 'static>) -> Response<String> {
    let details = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "Unknown panic message".to_string()
    };

    // sentry::capture_message(&details, sentry::Level::Fatal);
    tracing::error!("Panic: {}", details);

    let body = serde_json::json!({
        "error": {
            "kind": "panic",
            "details": details,
        }
    });
    let body = serde_json::to_string(&body).unwrap();

    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap()
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
    db: &DBPools,
    cancellation_token: CancellationToken,
) {
    loop {
        // For now this is just a single check, but in the future
        // we might have many checks; we first gather them and then write to the atomic bool once.
        let is_app_healthy = db.check_health().await;
        app_healthy_flag.store(is_app_healthy, std::sync::atomic::Ordering::Relaxed);

        select! {
            () = cancellation_token.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(10)) => {},
        }
    }
    // Set to unhealthy when shutting down
    app_healthy_flag.store(false, std::sync::atomic::Ordering::Relaxed);
}

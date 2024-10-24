use opsqueue::serve_producer_and_consumer;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::level_filters::LevelFilter;
use std::time::Duration;

pub const DATABASE_FILENAME: &str = "opsqueue.db";

#[tokio::main]
async fn main() {
    println!("Starting Opsqueue");

    let task_tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();


    // let subscriber = tracing_subscriber();
    // tracing::subscriber::set_global_default(subscriber).expect("Error setting up global tracing subscriber");
    let _ = setup_tracing();

    tracing::info!("Finished setting up tracing subscriber");


    let database_filename = DATABASE_FILENAME;

    opsqueue::ensure_db_exists(database_filename).await;
    let db_pool = opsqueue::db_connect_pool(database_filename).await;
    opsqueue::ensure_db_migrated(&db_pool).await;

    let server_addr = Box::from("0.0.0.0:3999");
    // let producer_server_addr = Box::from("0.0.0.0:3999");
    // let consumer_server_addr = Box::from("0.0.0.0:3998");
    let reservation_expiration = Duration::from_secs(60 * 60); // 1 hour

    // let consumer_server = opsqueue::consumer::server::serve(
    //     db_pool.clone(),
    //     consumer_server_addr,
    //     reservation_expiration,
    //     cancellation_token.clone(),
    //     task_tracker.clone(),
    // );
    // let consumer_server = opsqueue::consumer::server::serve(
    //     db_pool.clone(),
    //     consumer_server_addr,
    //     cancellation_token.clone(),
    //     reservation_expiration,
    // );
    // let producer_server = opsqueue::producer::server::serve(db_pool, producer_server_addr);

    // task_tracker.spawn(consumer_server);
    // tokio::spawn(producer_server);
    task_tracker.spawn(opsqueue::serve_producer_and_consumer(server_addr, db_pool, cancellation_token.clone(), reservation_expiration));

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to set up Ctrl+C signal handler");

    tracing::warn!("Opsqueue is shutting down");

    task_tracker.close();
    cancellation_token.cancel();
    task_tracker.wait().await;

    println!();
    println!("Opsqueue Stopped");
}

struct OtelGuard {}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider(); // Give OTel a tiny bit of time to gracefully shut down.
    }
}

#[must_use]
fn setup_tracing() -> OtelGuard {
    use tracing_subscriber::prelude::*;

    // By default log at INFO level (which includes the less verbose WARN and ERROR levels).
    // This can be overridden using the RUST_LOG env var.
    //
    // Some examples are `RUST_LOG=debug` (use DEBUG level everywhere) and `RUST_LOG="info, opsqueue=trace"` (use INFO level everywhere, but for content of the opsqueue crate, use the most verbose TRACE level).
    //
    // c.f. https://docs.rs/env_logger/latest/env_logger/#enabling-logging
    // and https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives
    let log_filter = tracing_subscriber::EnvFilter::builder().with_default_directive(LevelFilter::INFO.into()).from_env_lossy();

    tracing_subscriber::registry()
        .with(log_filter)
        .with(tracing_subscriber::fmt::layer().with_line_number(true).with_thread_ids(true).with_target(true))
        // .with(MetricsLayer::new(meter_provider.clone()))
        .with(tracing_opentelemetry::OpenTelemetryLayer::new(otel_tracer()))
        .init();

    OtelGuard{}
}

// We override the default error handler to log errors at a lower logging level,
// and this way make it less noisy for devs that are not actively running e.g. `jaeger` in development.
//
// This is based on https://github.com/open-telemetry/opentelemetry-rust/blob/8bd529a6d629aff7482b875cfc39275a8a71eaeb/opentelemetry/src/global/error_handler.rs#L56
//
// This is slightly suspect, as this usage of `log` might itself end up as a tracing event.
// As such, it really is only intended for development mode.
fn otel_debug_mode_error_handler<T: Into<opentelemetry::global::Error>>(err: T) {
    use opentelemetry::global::Error;
    match err.into() {
            Error::Trace(err) => log::debug!("OpenTelemetry trace error occurred. {}", err),
            Error::Propagation(err) => {
                log::debug!("OpenTelemetry propagation error occurred. {}", err)
            }
            other => log::debug!("OpenTelemetry error occurred. {}", other),
        }
}

/// Builds the tracer configuration for OpenTelemetry,
/// including the desired sampling rate and exporter to use.
fn otel_tracer() -> opentelemetry_sdk::trace::Tracer {
    use opentelemetry::trace::TracerProvider;
    let provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default()
                .with_sampler(opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(
                    0.01,
                ))))
                .with_id_generator(opentelemetry_sdk::trace::RandomIdGenerator::default())
                .with_resource(opentelemetry_resource()),
        )
        .with_batch_config(opentelemetry_sdk::trace::BatchConfig::default())
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .unwrap();

    opentelemetry::global::set_tracer_provider(provider.clone());

    // In debug builds, override the error handler, to avoid noisy logs when devs don't run Jaeger.
    #[cfg(debug_assertions)]
    let _ = opentelemetry::global::set_error_handler(otel_debug_mode_error_handler);

    provider.tracer("opsqueue")
}
fn opentelemetry_resource() -> opentelemetry_sdk::Resource {
    use opentelemetry_semantic_conventions::{
        attribute::{DEPLOYMENT_ENVIRONMENT_NAME, SERVICE_NAME, SERVICE_VERSION},
        SCHEMA_URL,
    };
    opentelemetry_sdk::Resource::from_schema_url(
        [
            opentelemetry::KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
            opentelemetry::KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
            opentelemetry::KeyValue::new(DEPLOYMENT_ENVIRONMENT_NAME, "develop"),
        ],
        SCHEMA_URL,
    )
}

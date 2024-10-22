use tokio_util::{sync::CancellationToken, task::TaskTracker};
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

    let producer_server_addr = Box::from("0.0.0.0:3999");
    let consumer_server_addr = Box::from("0.0.0.0:3998");
    let reservation_expiration = Duration::from_secs(60 * 60); // 1 hour

    let consumer_server = opsqueue::consumer::server::serve(
        db_pool.clone(),
        consumer_server_addr,
        reservation_expiration,
        cancellation_token.clone(),
        task_tracker.clone(),
    );
    let producer_server = opsqueue::producer::server::serve(db_pool, producer_server_addr);

    task_tracker.spawn(consumer_server);
    tokio::spawn(producer_server);

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

    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().with_line_number(true).with_thread_ids(true).with_target(true))
        // .with(MetricsLayer::new(meter_provider.clone()))
        .with(tracing_opentelemetry::OpenTelemetryLayer::new(otel_tracer()))
        .init();

    OtelGuard{}
}

/// Sets up the global tracing subscriber.
/// Current choices are based on the defaults described
/// in the Tokio tracing tutorial https://tokio.rs/tokio/topics/tracing
// fn tracing_subscriber() -> impl tracing::Subscriber {
//     // Start configuring a `fmt` subscriber
//     tracing_subscriber::fmt()
//     // The log level is set based on the `RUST_LOG` environment variable.
//     // ex: `RUST_LOG="debug"` shows all error, warn, info and debug logs (but no trace logs) across all crates.
//     // `RUST_LOG="opsqueue=trace"` shows _all_ logs for opsqueue, including trace logs.
//     .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
//     // Use a more compact, abbreviated log format
//     // .compact()
//     // Display source code file paths
//     .with_file(true)
//     // Display source code line numbers
//     .with_line_number(true)
//     // Display the thread ID an event was recorded on
//     .with_thread_ids(true)
//     // Don't display the event's target (module path)
//     .with_target(false)
//     // Build the subscriber
//     .finish()
// }

// Construct Tracer for OpenTelemetryLayer
fn otel_tracer() -> opentelemetry_sdk::trace::Tracer {
    use opentelemetry::trace::TracerProvider;
    let provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default()
                // Customize sampling strategy
                .with_sampler(opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(
                    0.01,
                ))))
                // If export trace to AWS X-Ray, you can use XrayIdGenerator
                .with_id_generator(opentelemetry_sdk::trace::RandomIdGenerator::default())
                .with_resource(opentelemetry_resource()),
        )
        .with_batch_config(opentelemetry_sdk::trace::BatchConfig::default())
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .unwrap();

    opentelemetry::global::set_tracer_provider(provider.clone());
    provider.tracer("tracing-otel-subscriber")
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

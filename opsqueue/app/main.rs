use clap::Parser;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, SdkTracerProvider};
use opsqueue::{common::submission::db::periodically_cleanup_old, config::Config, prometheus};
use std::{
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tracing::level_filters::LevelFilter;

fn main() {
    println!(
        "Starting Opsqueue {}\nHello, hello!",
        opsqueue::version_info()
    );

    // Sentry has to be initialized before starting the Tokio runtime
    let _sentry_guard = init_sentry();
    async_main()
}

#[tokio::main]
pub async fn async_main() {
    let _tracing_guard = setup_tracing();
    tracing::info!("Finished setting up tracing subscriber");

    let config = Box::leak(Box::new(Config::parse()));

    let server_addr = Box::from(format!("0.0.0.0:{}", config.port));
    let app_healthy_flag = Arc::new(AtomicBool::new(false));

    let cancellation_token = CancellationToken::new();

    // Set up Prometheus early because metrics that try to register before it is set up
    // will not be seen otherwise
    let prometheus_config = opsqueue::prometheus::setup_prometheus();

    let db_pool = tokio::time::timeout(
        Duration::from_secs(10),
        opsqueue::db::open_and_setup(&config.database_filename, config.max_read_pool_size),
    )
    .await
    .expect("Timed out while initiating the database");

    moro_local::async_scope!(|scope| {
        scope.spawn(opsqueue::server::serve_producer_and_consumer(
            config,
            &server_addr,
            &db_pool,
            config.reservation_expiration.into(),
            &cancellation_token,
            &app_healthy_flag,
            prometheus_config,
        ));

        let max_age = config.max_submission_age.into();
        scope.spawn(periodically_cleanup_old(db_pool.writer_pool(), max_age));

        scope.spawn(prometheus::periodically_calculate_scaling_metrics(
            &db_pool,
            &cancellation_token,
        ));

        // Set up complete. Start up watchdog, which will mark app healthy when appropriate
        scope.spawn(opsqueue::server::app_watchdog(
            app_healthy_flag.clone(),
            &db_pool,
            cancellation_token.clone(),
        ));

        tracing::info!(
            "Startup of Opsqueue ({}) complete.",
            opsqueue::version_info()
        );

        tokio::signal::ctrl_c()
            .await
            .expect("Failed to set up Ctrl+C signal handler");

        tracing::warn!("Opsqueue is shutting down");

        // Trigger graceful shutdown
        cancellation_token.cancel();

        // Gives things a little time to shut down, but not much :-)
        tokio::time::sleep(Duration::from_millis(500)).await;
        scope.terminate::<()>(()).await;
    })
    .await;

    println!();
    println!(
        "Graceful shutdown of Opsqueue {} completed.\nGoodbye!",
        opsqueue::version_info()
    );
}

/// Starts up the Sentry client to forward errors/panics to it.
///
/// SENTRY_DSN, SENTRY_ENVIRONMENT and SENTRY_RELEASE
/// are expected to be set as environment variables
/// (and if unset, Sentry support is turned off)
fn init_sentry() -> sentry::ClientInitGuard {
    let options = sentry::ClientOptions {
        // We want to send traces to whatever is configured for OpenTelemetry, *not* sentry:
        traces_sample_rate: 0.0,
        ..Default::default()
    };
    sentry::init(options)
}

struct OtelGuard {
    provider: SdkTracerProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        let _ = self.provider.force_flush();
        let _ = self.provider.shutdown();
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
    let log_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    let provider = otel_tracer_provider();
    let tracer = provider.tracer("opsqueue");

    tracing_subscriber::registry()
        .with(log_filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_line_number(true)
                .with_thread_ids(true)
                .with_target(true),
        )
        .with(tracing_opentelemetry::OpenTelemetryLayer::new(tracer))
        // While we don´t forward traces to Sentry, we do want info and above spans to show up as breadcrumbs
        // and error spans to show up as errors
        .with(sentry_tracing::layer())
        .init();

    OtelGuard { provider }
}

/// Builds the tracer configuration for OpenTelemetry,
/// including the desired sampling rate and exporter to use.
fn otel_tracer_provider() -> SdkTracerProvider {
    // Allow overriding the default trace sample rate using an environment variable.
    // By default, 1% is used.
    // Note that if a producer or consumer request arrives at Opsqueue
    // with the appropriate traceparent header set,
    // the trace will be sampled regardless of this value.
    let default_trace_sample_rate: f64 = std::env::var("OPSQUEUE_OTEL_DEFAULT_TRACE_SAMPLE_RATE")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(1.0);

    let exporter = SpanExporter::builder().with_http().build().unwrap();
    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
        default_trace_sample_rate,
    )));
    let provider: SdkTracerProvider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(opentelemetry_resource())
        .build();

    opentelemetry::global::set_tracer_provider(provider.clone());

    provider
}
fn opentelemetry_resource() -> opentelemetry_sdk::Resource {
    use opentelemetry_semantic_conventions::attribute::{
        DEPLOYMENT_ENVIRONMENT_NAME, SERVICE_NAME, SERVICE_VERSION,
    };
    opentelemetry_sdk::Resource::builder()
        .with_attributes([
            opentelemetry::KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
            opentelemetry::KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
            opentelemetry::KeyValue::new(DEPLOYMENT_ENVIRONMENT_NAME, "develop"),
        ])
        .build()
}

use axum_prometheus::{
    metrics::{describe_counter, describe_gauge, describe_histogram, gauge, Unit},
    metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle},
    utils::SECONDS_DURATION_BUCKETS,
    GenericMetricLayer, PrometheusMetricLayer, AXUM_HTTP_REQUESTS_DURATION_SECONDS,
};

use crate::db::DBPools;

pub const SUBMISSIONS_TOTAL_COUNTER: &str = "submissions_total_count";
pub const SUBMISSIONS_COMPLETED_COUNTER: &str = "submissions_completed_count";
pub const SUBMISSIONS_FAILED_COUNTER: &str = "submissions_failed_count";
pub const SUBMISSIONS_DURATION_COMPLETE_HISTOGRAM: &str = "submissions_complete_duration_seconds";
pub const SUBMISSIONS_DURATION_FAIL_HISTOGRAM: &str = "submissions_fail_duration_seconds";

pub const CHUNKS_COMPLETED_COUNTER: &str = "chunks_completed_count";
pub const CHUNKS_FAILED_COUNTER: &str = "chunks_failed_count";
pub const CHUNKS_RETRIED_COUNTER: &str = "chunks_retried_count";
pub const CHUNKS_SKIPPED_COUNTER: &str = "chunks_skipped_count";
pub const CHUNKS_TOTAL_COUNTER: &str = "chunks_total_count";
pub const CHUNKS_BACKLOG_GAUGE: &str = "chunks_in_backlog_count";

pub const CHUNKS_DURATION_COMPLETED_HISTOGRAM: &str = "chunks_duration_completed_seconds";
pub const CHUNKS_DURATION_FAILED_HISTOGRAM: &str = "chunks_duration_failure_seconds";

pub const RESERVER_RESERVATIONS_SUCCEEDED_COUNTER: &str = "reserver_reservations_succeeded_count";
pub const RESERVER_RESERVATIONS_FAILED_COUNTER: &str = "reserver_reservations_failed_count";
pub const RESERVER_CHUNKS_RESERVED_GAUGE: &str = "reserver_chunks_reserved_count";

pub const CONSUMERS_CONNECTED_GAUGE: &str = "consumers_connected_count";
pub const CONSUMER_FETCH_AND_RESERVE_CHUNKS_HISTOGRAM: &str =
    "consumer_fetch_and_reserve_chunks_duration_seconds";
pub const CONSUMER_COMPLETE_CHUNK_DURATION: &str = "consumer_complete_chunk_duration_seconds";
pub const CONSUMER_FAIL_CHUNK_DURATION: &str = "consumer_fail_chunk_duration_seconds";

pub fn describe_metrics() {
    describe_counter!(SUBMISSIONS_TOTAL_COUNTER, Unit::Count, "Total count of submissions (in backlog + completed + failed), i.e. total that ever entered the system");
    describe_counter!(
        SUBMISSIONS_COMPLETED_COUNTER,
        Unit::Count,
        "Number of submissions completed successfully"
    );
    describe_counter!(
        SUBMISSIONS_FAILED_COUNTER,
        Unit::Count,
        "Number of submissions failed permanently"
    );
    describe_histogram!(SUBMISSIONS_DURATION_COMPLETE_HISTOGRAM, Unit::Seconds, "Time between a submission entering the system and its final chunk being completed. Does not count failed submissions.");

    describe_counter!(
        CHUNKS_COMPLETED_COUNTER,
        Unit::Count,
        "Number of chunks completed"
    );
    describe_counter!(
        CHUNKS_FAILED_COUNTER,
        Unit::Count,
        "Number of chunks failed permanently (retries exhausted). Does not include skipped chunks"
    );
    describe_counter!(
        CHUNKS_RETRIED_COUNTER,
        Unit::Count,
        "Number of chunks that failed temporarily and will be retried"
    );
    describe_counter!(
        CHUNKS_SKIPPED_COUNTER,
        Unit::Count,
        "Number of chunks skipped (because another chunk in the submission failed)"
    );
    describe_counter!(CHUNKS_TOTAL_COUNTER, Unit::Count, "Total count of chunks (in backlog + completed + failed), i.e. total that ever entered the system");
    // We could calculate the backlog size from TOTAL - COMPLETED - FAILED - SKIPPED
    // but since it will commonly be used for checking whether we should autoscale,
    // it's much nicer to measure/expose it directly
    describe_gauge!(CHUNKS_BACKLOG_GAUGE, Unit::Count, "Number of chunks in the backlog. Note that this is a _gauge_ reflecting the accurate state of the DB");
    describe_histogram!(CHUNKS_DURATION_COMPLETED_HISTOGRAM, Unit::Seconds, "How long it took from the moment a chunk was reserved until 'complete_chunk' was called, as measured from Opsqueue (so including network overhead and reading/writing to the object_store to get the chunk data)");
    describe_histogram!(CHUNKS_DURATION_FAILED_HISTOGRAM, Unit::Seconds, "How long it took from the moment a chunk was reserved until 'fail_chunk' was called, as measured from Opsqueue (so including network overhead and reading/writing to the object_store to get the chunk data). Includes chunks that are retried.");

    describe_gauge!(RESERVER_CHUNKS_RESERVED_GAUGE, Unit::Count, "Number of chunks currently reserved by the reserver, i.e. being worked on by the consumers");

    describe_gauge!(
        CONSUMERS_CONNECTED_GAUGE,
        Unit::Count,
        "Number of healthy websocket connections between the system and consumers"
    );
    describe_histogram!(CONSUMER_FETCH_AND_RESERVE_CHUNKS_HISTOGRAM, Unit::Seconds, "Time spent by Opsqueue (SQLite + reserver) to reserve `limit` chunks for a consumer using strategy `strategy`");
    describe_histogram!(
        CONSUMER_COMPLETE_CHUNK_DURATION,
        Unit::Seconds,
        "Time spent by Opsqueue to mark a given chunk as completed"
    );
    describe_histogram!(
        CONSUMER_FAIL_CHUNK_DURATION,
        Unit::Seconds,
        "Time spent by Opsqueue to mark a given chunk as failed"
    );
}

pub type PrometheusConfig = (
    GenericMetricLayer<'static, PrometheusHandle, axum_prometheus::Handle>,
    PrometheusHandle,
);

#[must_use]
pub fn setup_prometheus() -> (
    GenericMetricLayer<'static, PrometheusHandle, axum_prometheus::Handle>,
    PrometheusHandle,
) {
    // PrometheusMetricLayer::pair()
    let metric_layer = PrometheusMetricLayer::new();
    // This is the default if you use `PrometheusMetricLayer::pair`.
    let metric_handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(AXUM_HTTP_REQUESTS_DURATION_SECONDS.to_string()),
            SECONDS_DURATION_BUCKETS,
        )
        .expect("Building Prometheus failed")
        .install_recorder()
        .expect("Installing global Prometheus recorder failed");

    describe_metrics();

    (metric_layer, metric_handle)
}

/// Initializes certain metrics that depend on the contents of the DB at app startup
///
/// Instead of asking the DB for a count very frequently, we only fetch the count at startup
/// and keep it up-to-date over the lifespan of the application
pub async fn prefill_special_metrics(db_pool: &DBPools) -> anyhow::Result<()> {
    let mut conn = db_pool.reader_conn().await?;
    let chunk_count: u64 = crate::common::chunk::db::count_chunks(&mut conn)
        .await?
        .into();
    gauge!(CHUNKS_BACKLOG_GAUGE).set(chunk_count as f64);

    Ok(())
}

/// Returns the number of seconds contained by this TimeDelta as f64, with nanosecond precision.
///
/// Adapted from https://doc.rust-lang.org/std/time/struct.Duration.html#method.as_secs_f64
pub fn time_delta_as_f64(td: chrono::TimeDelta) -> f64 {
    const NANOS_PER_SEC: f64 = 1_000_000_000.0;
    (td.num_seconds() as f64) + (td.subsec_nanos() as f64) / NANOS_PER_SEC
}

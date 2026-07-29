//! Helpers to read/write OpenTelemetry Tracing contexts from inside submissions stored in the queue
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::{Context, propagation::TextMapCompositePropagator};
use opentelemetry_http::{HeaderExtractor, HeaderInjector};
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use rustc_hash::FxHashMap;
use std::error::Error;

#[must_use]
pub fn context_from_headers(headers: &http::HeaderMap) -> Context {
    let propagator = propagator();
    propagator.extract(&HeaderExtractor(headers))
}

#[must_use]
pub fn context_to_headers(context: &Context) -> http::HeaderMap {
    let propagator = propagator();
    let mut headers = http::HeaderMap::default();
    propagator.inject_context(context, &mut HeaderInjector(&mut headers));
    headers
}

#[must_use]
pub fn current_context_to_json() -> String {
    use tracing::Span;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    context_to_json(&Span::current().context())
}

#[must_use]
pub fn context_to_json(context: &Context) -> String {
    let propagator = propagator();
    let mut map = CarrierMap::default();
    propagator.inject_context(context, &mut map);
    serde_json::to_string(&map).unwrap_or("{}".to_string())
}

#[must_use]
pub fn json_to_context(json: &str) -> Context {
    let propagator = propagator();
    serde_json::from_str(json).map_or(Context::new(), |hashmap: CarrierMap| {
        propagator.extract(&hashmap)
    })
}

#[must_use]
pub fn json_to_carrier(json: &str) -> CarrierMap {
    serde_json::from_str(json).unwrap_or_default()
}

pub type CarrierMap = FxHashMap<String, String>;

#[must_use]
pub fn propagator() -> TextMapCompositePropagator {
    TextMapCompositePropagator::new(vec![
        Box::new(BaggagePropagator::new()),
        Box::new(TraceContextPropagator::new()),
    ])
}

/// Convenient function for converting an error into `dyn Error` for tracing.
///
/// When logging an error, convert the error to `dyn Error` using this function
/// and assign it to the `error` field, rather than displaying the error, so
/// that the source chain of the error is preserved.
///
/// ```ignore
/// // Good:
/// tracing::error!(error = as_dyn_error(e), "operation failed");
///
/// // Bad:
/// tracing::error!("operation failed: {e}");
/// tracing::error!(error = %e, "operation failed");
/// ```
pub fn as_dyn_error<T>(err: &T) -> &(dyn 'static + Error)
where
    T: 'static + Error,
{
    err
}

/// [`anyhow::Error`] does not implement [`Error`], so this function is
/// separate from [`as_dyn_error`].
#[must_use]
pub fn anyhow_as_dyn_error(err: &anyhow::Error) -> &(dyn 'static + Error) {
    err.as_ref()
}

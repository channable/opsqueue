//! Helpers to read/write OpenTelemetry Tracing contexts from inside submissions stored in the queue
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::{propagation::TextMapCompositePropagator, Context};
use opentelemetry_http::{HeaderExtractor, HeaderInjector};
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use rustc_hash::FxHashMap;

pub fn context_from_headers(headers: &http::HeaderMap) -> Context {
    let propagator = propagator();
    propagator.extract(&HeaderExtractor(headers))
}

pub fn context_to_headers(context: &Context) -> http::HeaderMap {
    let propagator = propagator();
    let mut headers = Default::default();
    propagator.inject_context(context, &mut HeaderInjector(&mut headers));
    headers
}

pub fn current_context_to_json() -> String {
    use tracing::Span;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    context_to_json(&Span::current().context())
}

pub fn context_to_json(context: &Context) -> String {
    let propagator = propagator();
    let mut map = CarrierMap::default();
    propagator.inject_context(context, &mut map);
    serde_json::to_string(&map).unwrap_or("{}".to_string())
}

pub fn json_to_context(json: &str) -> Context {
    let propagator = propagator();
    serde_json::from_str(json)
        .map(|hashmap: CarrierMap| propagator.extract(&hashmap))
        .unwrap_or(Context::new())
}

pub fn json_to_carrier(json: &str) -> CarrierMap {
    serde_json::from_str(json).unwrap_or_default()
}

pub type CarrierMap = FxHashMap<String, String>;

pub fn propagator() -> TextMapCompositePropagator {
    TextMapCompositePropagator::new(vec![
        Box::new(BaggagePropagator::new()),
        Box::new(TraceContextPropagator::new()),
    ])
}

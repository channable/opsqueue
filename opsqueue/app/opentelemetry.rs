use std::backtrace::{Backtrace, BacktraceStatus};
use std::fmt::Display;
use std::io::IsTerminal;
use std::str::FromStr;

use opentelemetry::global::{self};
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};
use tracing::{error, warn};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, Registry};

const FORMAT_ENV_VAR: &str = "OPSQUEUE_LOG_FORMAT";

/// The log format chosen through `OPSQUEUE_LOG_FORMAT`. Not used when outputting to journald.
#[derive(Debug, Default)]
enum FormatOption {
    #[default]
    Full,
    Compact,
    Pretty,
}

impl FromStr for FormatOption {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("full") {
            Ok(Self::Full)
        } else if s.eq_ignore_ascii_case("compact") {
            Ok(Self::Compact)
        } else if s.eq_ignore_ascii_case("pretty") {
            Ok(Self::Pretty)
        } else {
            Err(s.to_owned())
        }
    }
}

impl Display for FormatOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FormatOption::Full => write!(f, "full"),
            FormatOption::Compact => write!(f, "compact"),
            FormatOption::Pretty => write!(f, "pretty"),
        }
    }
}

pub struct OtelGuard(SdkTracerProvider);
impl Drop for OtelGuard {
    #[allow(clippy::print_stderr)]
    fn drop(&mut self) {
        if let Err(e) = self.0.force_flush() {
            eprintln!("Failed to flush OpenTelemetry tracer: {e:?}");
        }
        if let Err(e) = self.0.shutdown() {
            eprintln!("Failed to shutdown OpenTelemetry tracer: {e:?}");
        }
    }
}

pub fn initialize_tracing(service_name: &'static str) -> Option<OtelGuard> {
    let provider = initialize_opentelemetry_provider(service_name);

    // When running locally we still want to also output to stdout.
    if true {
        let (format_option, invalid_format_str) = match std::env::var(FORMAT_ENV_VAR).ok() {
            None => (FormatOption::default(), None),
            Some(format_name) => match format_name.parse() {
                Ok(format_option) => (format_option, None),
                Err(_) => (FormatOption::default(), Some(format_name)),
            },
        };

        let layer = tracing_subscriber::fmt::layer().with_ansi(stdout_supports_colors());
        let format = tracing_subscriber::fmt::format();
        let formatted_layer = match format_option {
            FormatOption::Full => layer.boxed(),
            FormatOption::Compact => layer.event_format(format.compact()).boxed(),
            FormatOption::Pretty => layer.event_format(format.pretty()).boxed(),
        };
        // initialize_with_layer(layers.and_then(formatted_layer).boxed());
        initialize_with_layer(formatted_layer.boxed());

        if let Some(invalid_format_str) = invalid_format_str {
            warn!(
                invalid_format = invalid_format_str,
                "Invalid log format, falling back to '{:?}'.",
                FormatOption::default()
            );
        }
        // } else {
        //     initialize_with_layer(layers);
    }

    Some(OtelGuard(provider))
}

/// Initializes an OpenTelemetry tracer provider that exports traces using the OTLP protocol.
/// This function registers the provider as the global tracer provider.
fn initialize_opentelemetry_provider(service_name: &'static str) -> SdkTracerProvider {
    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to create span exporter");

    let resources = Resource::builder()
        .with_attributes(vec![
            opentelemetry::KeyValue::new(SERVICE_NAME, service_name),
            opentelemetry::KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
        ])
        .build();

    // Create a tracer provider with the exporter
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(otlp_exporter)
        .with_resource(resources)
        .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
        .build();

    global::set_tracer_provider(tracer_provider.clone());
    tracer_provider
}

// This layer determines where the logs are being written to, since we want to output to either
// the journal or to STDOUT
fn initialize_with_layer(layer: Box<dyn Layer<Registry> + Send + Sync>) {
    let log_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    tracing_subscriber::registry()
        .with(layer.boxed())
        // These layers compose through generic structs, so chaining the layers this way will
        // cause the filter to fire before `layer`
        .with(log_filter)
        .with(sentry_tracing::layer().boxed())
        .with(tracing_opentelemetry::OpenTelemetryLayer::new(
            global::tracer_provider().tracer("frida"),
        ))
        // This will panic if some external somehow already initialized tracing (which of course
        // should never happen unless external library code also initializes tracing)
        .init();
}

/// Sets up a panic handler that logs panics to the tracing logger.
pub fn initialize_panic_handler() {
    let default_panic_hook = std::panic::take_hook();

    std::panic::set_hook(Box::new(move |info| {
        if !tracing::event_enabled!(tracing::Level::ERROR) {
            return default_panic_hook(info);
        }

        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unnamed");
        let backtrace_string = match Backtrace::capture() {
            backtrace if backtrace.status() == BacktraceStatus::Disabled => String::from(
                "run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
            ),
            backtrace => backtrace.to_string(),
        };

        let msg = match info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => &**s,
                None => "Box<Any>",
            },
        };

        match info.location() {
            Some(location) => {
                error!(
                    target: "panic",
                    panic_message = msg,
                    panic_filename = location.file(),
                    panic_line = location.line(),
                    panic_column = location.column(),
                    thread = thread_name,
                    // This is the old single line format, which is a bit more difficult to read but
                    // a lot nicer to work with using line based tooling
                    "thread '{}' panicked at {}: {}\n\n{}",
                    thread_name,
                    location,
                    msg,
                    backtrace_string,
                );
            }
            None => {
                // Rust currently always captures a location for panics, so this branch should never
                // be hit
                error!(
                    target: "panic",
                    panic_message = msg,
                    thread = thread_name,
                    "thread '{}' panicked: {}\n\n{}",
                    thread_name,
                    msg,
                    backtrace_string,
                );
            }
        }
    }));
}

/// Whether or not to use colors when outputting to STDOUT. Considers the standard-ish `CLICOLOR`,
/// `CLICOLOR_FORCE`, and `NO_COLOR` environment variables, and whether or not STDOUT is attached to
/// a PTY or TTY.
///
/// <http://bixense.com/clicolors/>
#[allow(clippy::collapsible_if)]
fn stdout_supports_colors() -> bool {
    if let Ok(value) = std::env::var("CLICOLOR_FORCE") {
        if !value.is_empty() && value.trim() != "0" {
            return true;
        }
    }

    if let Ok(value) = std::env::var("NO_COLOR") {
        if !value.is_empty() && value.trim() != "0" {
            return false;
        }
    }

    if let Ok(value) = std::env::var("CLICOLOR") {
        if value.trim() == "0" {
            return false;
        }
    }

    // If `CLICOLOR` is unset or set to a truthy value, and colors aren't forced, then terminal
    // support determines whether or not colors are used
    std::io::stdout().is_terminal()
}

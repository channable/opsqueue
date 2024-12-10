import opentelemetry
import opentelemetry.propagate
from opentelemetry.context import Context


def current_opentelemetry_tracecontext_to_carrier() -> dict[str, str]:
    """
    Converts the current trace+span in Python
    to its serialized 'TextMap" carrier format
    """
    carrier: dict[str, str] = {}
    otel_propagator().inject(carrier)
    return carrier


def carrier_to_opentelemetry_tracecontext(carrier: dict[str, str]) -> Context:
    """
    Converts a serialized "TextMap" carrier
    back into a span-context in Python.

    This can then be passed as 'parent context'
    when creating a new span.
    """
    return otel_propagator().extract(carrier)


def otel_propagator() -> opentelemetry.propagators.textmap.TextMapPropagator:
    """
    We use the default propagate which is configurable using an env var.
    Opsqueue supports the W3C trace context and W3C baggage
    """
    return opentelemetry.propagate.get_global_textmap()

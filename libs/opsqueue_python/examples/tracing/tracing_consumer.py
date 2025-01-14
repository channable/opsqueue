import logging
from opsqueue.consumer import ConsumerClient, Strategy

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    # ConsoleSpanExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


def set_up_global_tracer() -> None:
    """
    This is usually called once per app, at startup time.
    """
    resource = Resource(
        attributes={SERVICE_NAME: "tracing_with_opsqueue_example_consumer"}
    )
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter())
    provider.add_span_processor(processor)

    trace.set_tracer_provider(provider)


def run_consumer() -> None:
    tracer = trace.get_tracer("tracing_consumer")

    @tracer.start_as_current_span("incrementer")
    def incrementer(data: int) -> int:
        import time
        import random

        time.sleep(0.01)

        # Let's make the trace more spicy
        # by crashing every so often:
        if random.randrange(0, 50) == 0:
            raise KeyError("Simulated crash")

        return data + 1

    client = ConsumerClient("localhost:3999", "file:///tmp/opsqueue/tracing_example")
    client.run_each_op(incrementer, strategy=Strategy.Random())


def main() -> None:
    set_up_global_tracer()
    run_consumer()


if __name__ == "__main__":
    main()

import logging
import typing
import opentelemetry
import opentelemetry.context
from opentelemetry.context import Context

import contextlib
from typing import Optional, Generator
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    # ConsoleSpanExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from opsqueue.producer import ProducerClient

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


def set_up_global_tracer() -> None:
    """
    This is usually called once per app, at startup time.
    """
    resource = Resource(
        attributes={SERVICE_NAME: "tracing_with_opsqueue_example_producer"}
    )
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter())
    provider.add_span_processor(processor)

    trace.set_tracer_provider(provider)


def do_something() -> None:
    with trace.get_tracer(__name__).start_as_current_span("do_something"):
        with added_baggage(baggage={"app_mode": "preview"}):
            # print(opentelemetry.baggage.get_all())

            client = ProducerClient(
                "localhost:3999", "file:///tmp/opsqueue/tracing_example"
            )

            input_iter = range(0, 100)
            output_iter = client.run_submission(input_iter, chunk_size=10)

            # Now do something with the output:
            # for x in output_iter:
            #    print(x)
            print(sum(output_iter))


def main() -> None:
    set_up_global_tracer()
    do_something()


@contextlib.contextmanager
def added_baggage(
    baggage: Optional[dict[str, str]] = None,
    context: Optional[Context] = None,
) -> Generator[None, None, None]:
    attached_context_tokens: list[Context] = list()

    if baggage:
        for key, value in baggage.items():
            attached_token = opentelemetry.baggage.set_baggage(key, value, context)
            attached_context_tokens.append(
                typing.cast(Context, opentelemetry.context.attach(attached_token))
            )

    try:
        yield
    finally:
        for attached_token in attached_context_tokens:
            opentelemetry.context.detach(attached_token)


if __name__ == "__main__":
    main()

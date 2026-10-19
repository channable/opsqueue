The Python client library for the Opsqueue lightweight batch processing queue system.

Find the full README with examples at https://github.com/channable/opsqueue

## Getting Started:

### 1.  Grab the `opsqueue` binary and the Python client library

1. Install the Opsqueue binary, using `cargo install opsqueue` (if you do not have Cargo/Rust installed yet, follow the instructions at https://rustup.rs/ first)
2. Install the Python client using `pip install opsqueue`, `uv install opsqueue` or similar.

### 2. Create a `Producer`

```python
import logging
from opsqueue.producer import ProducerClient
from collections.abc import Iterable

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)


def file_to_words(filename: str) -> Iterable[str]:
    """
    Iterates over each word and inter-word whitespace strings in a file
    while keeping at most one line in memory at a time.
    """
    with open(filename) as input_file:
        for line in input_file:
            for word in line.split():
                yield word


def print_words(words: Iterable[str]) -> None:
    """
    Prints all words and inter-word whitespace tokens
    without first loading the full string into memory
    """
    for word in words:
        print(word, end="")


def main() -> None:
    client = ProducerClient("localhost:3999", "file:///tmp/opsqueue/capitalize_text/")
    stream_of_words = file_to_words("lipsum.txt")
    stream_of_capitalized_words = client.run_submission(
        stream_of_words, chunk_size=4000
    )
    print_words(stream_of_capitalized_words)


if __name__ == "__main__":
    main()
```

### 3. Create a `Consumer`

```python
import logging
from opsqueue.consumer import ConsumerClient, Strategy

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


def capitalize_word(word: str) -> str:
    output = word.capitalize()
    # print(f"Capitalized word: {word} -> {output}")
    return output


def main() -> None:
    client = ConsumerClient("localhost:3999", "file:///tmp/opsqueue/capitalize_text/")
    client.run_each_op(capitalize_word, strategy=Strategy.Random())


if __name__ == "__main__":
    main()
```


4. Run the Producer, queue and Consumer

- Run `opsqueue`.
- Run `python3 capitalize_text_consumer.py` to run a consumer. Feel free to start multiple instances of this program to try out consumer concurrency.
- Run `python3 capitalize_text_producer.py` to run a producer.

The order you start these in does not matter; systems will reconnect and continue after any kind of failure or disconnect.

By default the queue will listen on `http://localhost:3999`. The exact port can of course be changed.
Producer and Consumer need to share the same object store location to store the content of their submission chunks.
In development, this can be a local folder as shown in the code above.
In production, you probably want to use Google's GCS, Amazon's S3 or Microsoft's Azure buckets.

Please tinker with above code!
If you want more logging to look under the hood, run `RUST_LOG=debug opsqueue` to enable extra logging for the queue.
The Producer/Consumer will use whatever log level is configured in Python.

## PostgreSQL Extension

The PostgreSQL extension can be used directly from Python without running the Opsqueue service. Install the optional driver with `pip install 'opsqueue[postgres]'`, then pass a PostgreSQL DSN to the PostgreSQL clients:

Run the producer and consumer snippets in separate processes:

```python
from opsqueue.consumer import PostgresConsumerClient
from opsqueue.producer import PostgresProducerClient

dsn = "postgresql://localhost/mydb"

with PostgresProducerClient(dsn) as producer:
    submission_id = producer.insert_submission(range(100), chunk_size=10)

with PostgresConsumerClient(dsn) as consumer:
    consumer.run_each_op(lambda value: value + 1)
```

The PostgreSQL clients store input and output chunks in the extension's `bytea` columns, so they do not take an object-store URL. The consumer accepts `oldest`, `newest`, `random`, or `prefer_distinct` as its `strategy`; for `prefer_distinct`, provide `prefer_distinct_keys` and optionally `underlying_strategy`. Keep `lease_seconds` longer than the time a callback can take, or call `consumer.renew_chunk(submission_id, chunk_index)` during long-running work.

More examples can be found in `./libs/opsqueue_python/examples/`


## More Info

Find the full README with examples at https://github.com/channable/opsqueue

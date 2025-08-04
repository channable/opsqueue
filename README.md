# Opsqueue - A lightweight batch processing queue for the heaviest loads.
[![Crates.io Version](https://img.shields.io/crates/v/opsqueue?label=opsqueue%20(binary))](https://crates.io/crates/opsqueue) [![PyPI - Version](https://img.shields.io/pypi/v/opsqueue?label=Python%20client%20library)](https://pypi.org/project/opsqueue/)

## Why opsqueue?

The specific advantages of opsqueue are:

- Lightweight: small codebase, written in Rust, minimal dependencies
- Optimized for batch processing: we prioritize throughput over latency
- Built to scale to billions of operations
- Built with reliable building blocks: Rust, SQLite, Object Storage (such as S3 or GCS)
- Operationally simple: single binary, embedded database, minimal configuration
- Scales horizontally: you can have many consumers processing work in parallel
- Very flexible: you have full control over how you produce and consume operations. We use a novel prioritization approach where decisions can be made in the middle of ongoing work.

`opsqueue` is a good choice if you have a use case where you first generate a few million operations (an "operation" is any task that can be executed within a few seconds) and then later execute those operations.

## Getting Started:

### 1.  Grab the `opsqueue` binary and the Python client library

1. Install the Opsqueue binary, using `cargo install opsqueue` (if you do not have Cargo/Rust installed yet, follow the instructions at https://rustup.rs/ first) ([Rust crate page](https://crates.io/crates/opsqueue))
2. Install the Python client using `pip install opsqueue`, `uv install opsqueue` or similar. ([Pypi package page](https://pypi.org/project/opsqueue/))

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
    stream_of_capitalized_words = client.run_submission(stream_of_words, chunk_size=4000)
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

More examples can be found in `./libs/opsqueue_python/examples/`

## Project structure

The majority of Opsqueue's code is written in Rust.
There are four main components:

* The Rust library ('crate') 'opsqueue', found in `./opsqueue/src`. This contains the parts of opsqueue that are shared between the different compilation targets. The majority of the code lives here.
* The binary executable program `opsqueue`, found in `./opsqueue/app/`.
* The client libraries found in `./libs`. These are thin FFI wrappers around the Opsqueue crate. Each of these libraries contains a:
  * Producer Client (used to generate and send work to Opsqueue, and optionally receive results)
  * Consuumer Client (used to execute chunks of work that was sent to Opsqueue)

## For Nix users: Including Opsqueue via Nix

Opsqueue's client libraries and binary itself are also available through `niv`.

1. Add opsqueue to your `nix/sources.json`, possibly by using `niv add https://github.com/channable/opsqueue`
2. Package the now available `opsqueue` library as part of your overlay, using e.g.

```nix
opsqueue = self.callPackage (sources.opsqueue + "/libs/opsqueue_python/opsqueue_python.nix") { };
```

## For devs modifying Opsqueue: Building, running, testing

The [just](https://github.com/casey/just) command runner is used to wrap common commands.
Just is very similar to Make, but simpler.
Commands are defined in the justfile.

To make it easier to have a well-defined development environment,
build-dependencies are managed by [Nix](https://nixos.org/download/) and [direnv](https://github.com/direnv/direnv). While it is possible to install `just` + `cargo` + `python` + `maturin` + `pyO3` + various linters and related tooling manually, it is strongly recommended to use Nix + Direnv instead.

Run

```bash
just
```
to see the list of supported commands.

### Building

To build a development version (of the binary and the rust-side of all client libraries):

```bash
# Build everything (in dev mode):
just build

# Or, only building the executable:
just build-bin

# Or, only building the Python client library:
just build-python
```

Those last two commands also accept extra parameters.

For example, to build in release mode:

```bash
just build-bin --profile release # Args passed to `cargo`

just build-python --release # Args passed to `maturin develop`
```

### Building release builds with Nix

To build a production version (of both the binary and the client libraries) with the same Nix build setup that is also used on CI/CD, instead use:

```bash
# Build everything:
just nix-build

# Or, only building the executable:
just nix-build-bin

# Or, only building the Python client library:
just nix-build-python
```

These commands will print the resulting store paths to STDOUT.

### Testing

```bash
# To run all tests:
just test

# Or, to run only (Rust) unit tests:
just test-unit

# Or, to run only (Python) integration tests:
just test-integration
```

For some Rust unit tests, we use the [insta](https://insta.rs/docs/quickstart/) golden test library.
If golden tests are failing, you can use the `cargo insta` subcommand to review failed golden test snapshots; ([installation instructions](https://insta.rs/docs/cli/)).

### Running

To run the main `opsqueue` executable:

```bash
# To build-and-run the executable in dev mode:
just run -- --maybe --some --arguments

# or:
just build
./target/debug/opsqueue

# or, in release mode (faster and smaller executable, identical to what will run in production):
just build --profile release
./target/release/opsqueue
```

## Lints and checks

Simple lints can be run using `just lint-light`.
This will only execute lints that can run on individual files, and only run them
on the files that you changed since the last commit.
It should complete within a second.
Run `just lint-light --all` to run them on _all_ files.

The heavy lint passes, which do static analysis of the full codebase,
can be run using `just lint-heavy`.

Both `lint-light` and `lint-heavy` might modify files if their complaints are auto-fixable.

You can also run the full set of lints using `just lint`.
The linter also runs on CI.

## Database migrations

Opsqueue uses Sqlite as backing data store.
The [sqlx](https://github.com/launchbadge/sqlx/) library is used to manage this database structure and migrations.
Sqlx ensures at Rust compile-time that the queries are valid,
by connecting (at compile-time!) to the `./opsqueue/opsqueue_example_database_schema.db` Sqlite DB file.
Therefore, this file is checked in into git.

In select cases (pun intended) it might be necessary to run the `sqlx-cli` tool,
especially when creating a _new_ database migration. [Detailed usage notes of sqlx-cli can be found here](https://github.com/launchbadge/sqlx/blob/main/sqlx-cli/README.md#enable-building-in-offline-mode-with-query).

These checks happen at _compile-time_ for the opsqueue binary.

The migrations (in the `./opsqueue/migrations` subfolder) will become part of the compiled `opsqueue` binary.

When _running_ the `opsqueue` binary, it will automatically on startup:

* Create an `opsqueue.db` Sqlite database file in the current working directory if it did not exist,
* Apply any new migrations to make sure the database file has the most up-to-date format.

## Python client library

Currently, making the Rust FFI library usable from python is done using `maturin`.

Building the latest Python library using `just build-python` will call `maturin develop` on a mostly-empty Python .venv internally. This allows you to use `ipython` or run any of the examples in `libs/opsqueue_python` directly from that directory.

Changes to the Python code will immediately be picked up.
But note that the `just build-python` / `maturin develop` step needs to be repeated **after making any changes to the Rust code**.


[Maturin usage guide](https://www.maturin.rs/tutorial).

See the `./libs/opsqueue_python/examples` directory for a bunch of examples of using Opsqueue from Python.

## Running Python integration tests

You can run (only) the Python integration tests using
```bash
just test-integration`
```

This will set up the required steps below automatically.
Any arguments passsed are passed on to `pytest`, e.g.:

```bash
just test-integration -vvvvvv -s -k
```

Directly invoking Pytest is possible, **but be sure you use the Pytest from inside the special maturin virtual env**. Specifically:
1. Go to `./libs/opsqueue_python`
2. Make sure you run `maturin develop` so the code is up-to-date
3. Run `pytest` from this directory with all options you like.

**Be aware that this `pytest` is part of the maturin virtual env**.
That is the only way to allow it to see the development artefacts from `maturin develop`.
Therefore, do _not_ try to run `pytest` from another directory, it will not work (it will complain about not being able to find the `opsqueue_python` module).

## Running Litestream

To locally test Litestream DB-replication:

* open a first shell and run Minio, [following the 'setting up Minio step' from the Litestream getting started page](https://litestream.io/getting-started/#setting-up-minio). (NOTE: In the future we'll be able to use the [testing GCS bucket](https://github.com/channable/devops/issues/10948))
* open a second shell and run `nix-shell -p litestream`.
* In this shell, run

```bash
export LITESTREAM_ACCESS_KEY_ID=minioadmin
export LITESTREAM_SECRET_ACCESS_KEY=minioadmin
```

* Finally, run `litestream replicate opsqueue.db s3://mybkt.localhost:9000/opsqueue.db`
* And now, exercise the DB by running the main opsqueue binary and sending it work.

# A brief overview of Opsqueue's architecture

Opsqueue consists of three independently running parts: the Producer, the Queue (the 'opsqueue executable'), and the Consumer.

The producer and the consumer are pieces of code that you write (in e.g. Python), which use the Opsqueue client library to communicate with the queue.

## The Producer

The producer is responsible for building ('generating') an iterator of operations (a 'submission'). The client library can then be invoked to upload these to object storage (e.g. GCS) and submit the metadata of this submission to the queue.

Then, the producer will wait until the submission is done (using short-polling until the status of the submission has changed), after which it will receive back an iterator of results.
If the submission failed, instead the producer will receive a failure result instead. In Python this is raised as a `SubmissionFailed` exception.

## The Consumer

The consumer on the other hand will grab chunks of operations from the queue. Grabbing chunks is implemented in the client library. The code that you need to write,
is what happens to each of the operations (how to 'execute' them) and return an operation-result.

The consumer can use a _Strategy_ to indicate which kind of submission it would prefer to work on. This allows consumers to implement more sophisticated fairness methodologies.
Currently, a consumer can only indicate 'oldest first', 'newest first' or 'random'. In the near future, they will also be able to use strategies like 'prefer from a distinct user' (where the user ID is something that is set as part of the metadata of the submission when the producer sends it to the queue).

When picking up a chunk of operations from the queue, a consumer first _reserves_ the chunk and then downloads its contents from object storage. The queue guarantees that no other consumer will start working on a reserved chunk.
When the consumer is done with the chunk, it uploads the results back to object storage and then marks the chunk _as completed_ for the queue.
A consumer can also mark a chunk as _failed_, in which case the chunk's retry counter will increment inside the queue. After this, the chunk is back open for being reserved by another consumer. If the reservation counter is too high (default: 10), the chunk will _permanently fail_ and the full submission will fail (and all of the remaining chunks removed from the backlog).
Were a consumer to _raise an exception_ or _outright crash_ or _have network problems_, then the chunk(s) it is working on will similarly be un-reserved by the queue. See the heartbeating section below for details.

### Idempotency

In the event of a consumer crash or (ephemeral) network problems, we do not want work to get lost. The opsqueue system takes the 'at least once' approach (rather than the 'at most once' approach). This means that your consumers **must be idempotent**. They have to handle the possibility of (part of a) chunk being re-executed multiple times.

## API connections

Under the hood, the producer and the queue talk with each other using a JSON-REST API over HTTP. Users of opsqueue don't need to think about this, as this is abstracted behind the client library.

The communication between the consumer and the queue on the other hand is done in COBR over a persistent WebSocket connection. A heartbeating protocol is used to ensure that a closed or broken connection is detected early. The goal is that the system will recognize and recover from network problems or crashed consumers within seconds.
Similarly, as a user no detailed understanding of this should be necessary as it is abstracted away inside the client library.

### Consumer <-> Opsqueue Heartbeating

For heartbeating between the queue and the consumer, the following approach is used:
- Every 5 seconds (configurable), if no other message was sent/received on this connection, the queue will send a websocket 'PING' message
- Whenever a PING is received, the consumer client will respond with a 'PONG' (This is builtin behaviour of the websocket protocol)
- Whenever the queue or the consumer client receives any message (including a PING or PONG), the heartbeat timer is reset
- If it took more than 5 seconds (configurable) to receive the last heartbeat, the 'missed heartbeats' counter is incremented
- If the 'missed heartbeats' counter is > 3 (configurable), the connection is considered unhealthy, and the connection is closed.
  - The consumer: Upon a connection being closed, ongoing work is dropped. After that, the consumer will attempt to reconnect with the queue
  - The queue: Upon a connection being closed, work reserved by a consumer is un-reserved and may be picked up by another consumer

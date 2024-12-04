# Opsqueue

Making the big work horizontally scalable.

Opsqueue does this by being:

* Dead simple
* Super lightweight
* Highly scalable
* Very Flexible

## Why opsqueue?

Specific advantages for opsqueue:

* Small codebase that we fully understand
* Full control to do exactly what you want
* One standardized queuing system that can be reused again and again
* A single way to implement monitoring, alerting, and debugging workflows

## Project structure

The majority of Opsqueue's code is written in Rust.
There are four main components:

* The Rust library ('crate') 'opsqueue', found in `./opsqueue/src`. This contains the parts of opsqueue that are shared between the different compilation targets. The majority of the code lives here.
* The binary executable program `opsqueue`, found in `./opsqueue/app/`.
* The producer libraries found in `./opsqueue_producer`. These contain thin FFI wrappers around the Opsqueue crate
* The consumer libraries found in `./opsqueue_consumer`. These contain thin FFI wrappers around the opsqueue crate

## Building, running, testing

The builds are managed using Cargo + Maturin in development, and Nix for production release builds.

### Building

To build a development version (of the binary and the rust-side of all client libraries):

```bash
cargo build
```

To build a production version (of both the binary and the client libraries) with the same Nix build setup that is also used on CI/CD, instead use:

```bash
# Build everything:
./build.py build

# Or, only building the executable:
./build.py build opsqueue

# Or, only building the Python client library:
./build.py build opsqueue_python
```

### Testing

```bash
# To run all tests:
./build.py test

# Or, to run only (Rust) unit tests:
./build.py test unit

# Or, to run only (Python) integration tests:
./build.py test integration
```

### Running

To run the main `opsqueue` executable:

```bash
# To build-and-run the executable in dev mode:
./build.py run -- --maybe --some --arguments

# or:
cargo build
./target/debug/opsqueue

# or, in release mode (faster and smaller executable, identical to what will run in production):
cargo build --profile release
./target/release/opsqueue
```

### Linting

To run code-style checks, static analyses and formatting:

```bash
# Run all checks and fix those that can be fixed automatically:
./build.py check style --fix

# Check but don't fix (used in CI):
./build.py check style
```

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

If you want to create a temporary Python environment with the producer library
or consumer library in scope,
you can go to the `libs/opsqueue_python` directory
and run:

```bash
# NOTE: we depend on `direnv` to load a (mostly empty!) Python virtualenv, as it is a requirement for the next step.
# c.f. `./libs/opsqueue_python/.setup_local_venv.sh`
maturin develop # or `maturin develop -r` to run in release mode.
# Now, you can use python or ipython or whatever and access
# the `opsqueue_producer` resp. `opsqueue_consumer` module.
ipython
```
Changes to the Python code will immediately be picked up.
But note that the `maturin develop` step needs to be repeated **after making any changes to the Rust code**.

For full/final builds, just use Nix (with the `./build.py` commands above) which will call `maturin build -r` internally.

[Maturin usage guide](https://www.maturin.rs/tutorial).

See the `./libs/opsqueue_python/examples` directory for a bunch of examples of using Opsqueue from Python.

## Running Python integration tests

You can run (only) the Python integration tests using
`./build.py test integration`

This will set up the required steps below automatically.
The command accepts extra arguments after a `--` and passes those on to `pytest` unchanged, e.g.:

```bash
./build.py test integration -- -vvvvvv -s -k
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

The producer is responsible for building ('generating') an iterator of operations (a 'submission'). The client library can then be invoked to upload these to object storage (e.g. GCS) and submit the metadata of this submission to the queue.

Then, the producer will wait until the submission is done (using short-polling until the status of the submission has changed), after which it will receive back an iterator of results.

The consumer on the other hand will grab chunks of operations from the queue. Grabbing chunks is implemented in the client library. The code that you need to write,
is what happens to each of the operations (how to 'execute' them) and return an operation-result.

The consumer can use a _Strategy_ to indicate which kind of submission it would prefer to work on. This allows consumers to implement more sophisticated fairness methodologies. 
Currently, a consumer can only indicate 'oldest first', 'newest first' or 'random'. In the near future, they will also be able to use strategies like 'prefer from a distinct user' (where the user ID is something that is set as part of the metadata of the submission when the producer sends it to the queue). 

Under the hood, the producer and the queue talk with each other using a JSON-REST API over HTTP. Users of opsqueue don't need to think about this, as this is abstracted behind the client library.

The communication between the consumer and the queue on the other hand is done in COBR over a persistent WebSocket connection. A heartbeating protocol is used to ensure that a closed or broken connection is detected early. The goal is that the system will recognize and recover from network problems or crashed consumers within seconds.

## Heartbeating

To keep 

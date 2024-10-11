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

# Project structure

The majority of Opsqueue's code is written in Rust.
There are four main components:

- The Rust library ('crate') 'opsqueue', found in `./opsqueue/src`. This contains the parts of opsqueue that are shared between the different compilation targets. The majority of the code lives here.
- The binary executable program `opsqueue`, found in `./opsqueue/app/`.
- The producer libraries found in `./producer_libs`. These contain thin FFI wrappers around the Opsqueue crate
- The consumer libraries found in `./consumer_libs`. These contain thin FFI wrappers around the opsqueue crate

## Building, running, testing

To build (the binary and the rust-side of all producer/consumer libraries):

```bash
cargo build
```

To test (the binary and the rust-side of all producer/consumer libraries):

```bash
cargo test
```

To run the main `opsqueue` executable:

```bash
cargo run

# or:
cargo build
./target/debug/opsqueue

# or, in release mode (faster and smaller executable, identical to what will run in production):
cargo build --profile release
./target/release/opsqueue
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
- Create an `opsqueue.db` Sqlite database file in the current working directory if it did not exist,
- Apply any new migrations to make sure the database file has the most up-to-date format.

## Python producer/consumer libraries

Currently, making the Rust FFI library usable from python is done using `maturin`.

If you want to create a temporary Python environment with the producer library
or consumer library in scope,
you can go to the respective `producer_libs/python` or `consumer_libs/python` directory
and run:

```bash
maturin develop
# Now, you can use python or ipython or whatever and access
# the `opsqueue_producer` resp. `opsqueue_consumer` module.
ipython
```

If building a full Python wheel, use `maturin build` instead.

[Maturin usage guide](https://www.maturin.rs/tutorial).



## Running Litestream

To locally test Litestream DB-replication:  
- open a first shell and run Minio, [following the 'setting up Minio step' from the Litestream getting started page](https://litestream.io/getting-started/#setting-up-minio). (NOTE: In the future we'll be able to use the [testing GCS bucket](https://github.com/channable/devops/issues/10948))
- open a second shell and run `nix-shell -p litestream`.
- In this shell, run
```bash
export LITESTREAM_ACCESS_KEY_ID=minioadmin
export LITESTREAM_SECRET_ACCESS_KEY=minioadmin
```
- Finally, run `litestream replicate opsqueue.db s3://mybkt.localhost:9000/opsqueue.db`

- And now, exercise the DB by running the main opsqueue binary and sending it work. 

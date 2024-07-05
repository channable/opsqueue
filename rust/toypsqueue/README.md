# Before the first run

We test the queries used in the program at compile time.
This means that to compile the test program for the first time, 
we need to ensure that a SQLite database with the desired schema is available:

```bash
cargo install sqlx-cli --no-default-features --features sqlite
```

```bash
sqlx database create
sqlx migrate run
```

This is only necessary for the initial build.
Once the binary is compiled and placed somewhere else, it will happily create a SQLite DB (and/or migrate an existing Sqlite DB to the latest schema) in its directory on startup.

# Running the test program

Simply call `cargo run`.
This will:
- Ensure a local Sqlite DB called `opsqueue.db` exists
- Ensure the latest migrations (from the `./migrations` subdir, which gets embedded in the final executable!) are applied to this DB
- Finally, insert a large amount of submissions (with many chunks), from multiple different writer threads. Since Sqlite only accepts one writer at a time, the writer threads will have to wait for each other; This example tests insertion speed and writer fairness.

Feel free to tinker with the settings in `app/main.rs` to change the behaviour of this example script.

# Running Litestream

To locally test Litestream replication:  
- open a first shell and run Minio, [following the 'setting up Minio step' from the Litestream getting started page](https://litestream.io/getting-started/#setting-up-minio). (NOTE: In the future we'll be able to use the [testing GCS bucket](https://github.com/channable/devops/issues/10948))
- open a second shell and run `nix-shell -p litestream`.
- In this shell, run
```bash
export LITESTREAM_ACCESS_KEY_ID=minioadmin
export LITESTREAM_SECRET_ACCESS_KEY=minioadmin
```
- Finally, run `litestream replicate opsqueue.db s3://mybkt.localhost:9000/opsqueue.db`

Now, exercise the database by using e.g. `cargo run` and/or running some of the benchmarks.

# Benchmarking

The benchmarks will fail if the DB was not set up before.
To set up the DB, run plain `cargo run` at least once. (It is not necessary to let `cargo run` finish, only the DB creation + migrations are needed)

Note that for simplicity and tinkering's sake, the current benchmarks use the DB as-is;
this means  that if you add more data to the DB by running the insertion benchmark or `cargo run`, benchmarking results might be affected.

Running the benchmarks can be done using `cargo bench` to run all of them or `cargo bench benchmarkname` to run a specific one.

When done, a HTML report with nice graphs is written to `./target/criterion/report/index.html`.

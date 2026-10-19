# Opsqueue PostgreSQL extension

This crate adds a PostgreSQL-native queue API alongside the existing Opsqueue service. PostgreSQL stores submission state, chunk payloads, retry history, and reservation leases. Consumers claim work using `FOR UPDATE SKIP LOCKED`; expired leases become claimable again.

The extension currently targets PostgreSQL 16 through `pgrx` 0.19.3. It does not replace the HTTP/WebSocket service or migrate the Rust/Python clients yet. Payloads can be stored directly in PostgreSQL as `bytea`, or represented by a `prefix` and fetched from an external object store by the client.

## Build and install

Install `cargo-pgrx` 0.19.3 and register the local PostgreSQL 16 installation once:

```bash
cargo pgrx init --pg16 "$(command -v pg_config)"
```

Then, from this directory, build and start a pgrx-managed PostgreSQL instance:

```bash
cargo pgrx run pg16
```

In the launched database, create the extension:

```sql
CREATE SCHEMA opsqueue;
CREATE EXTENSION opsqueue_pg;
```

The extension installs its objects in the `opsqueue` schema.

## SQL API

- `opsqueue.submit(...)` inserts a submission and optionally its `bytea[]` chunks. Use `p_initial_status => 'paused'` to hold work until explicitly unpaused.
- `opsqueue.reserve(worker_uuid, limit, lease, strategy, prefer_distinct_keys, underlying_strategy)` atomically claims chunks and returns their payload and submission metadata. Strategies are `oldest`, `newest`, `random`, and `prefer_distinct`.
- `opsqueue.renew(...)` extends a worker's lease.
- `opsqueue.complete(...)` stores a result and marks the submission completed when its final chunk completes.
- `opsqueue.fail(...)` records a retry and returns the chunk to the queue, or fails the submission after `p_max_retries`.
- `opsqueue.status(id)`, `opsqueue.results(id)`, `opsqueue.failure_history(id)`, `lookup_by_prefix(prefix)`, `lookup_by_metadata(jsonb, limit)`, `count_in_progress()`, and `count_completed()` provide producer-side queries.
- `opsqueue.cancel(id)` cancels an in-progress or paused submission; `opsqueue.unpause(id)` makes paused work available.

Each consumer must use a stable, unique UUID as its worker ID and renew leases for work that may exceed the lease interval. A lost connection is recovered by lease expiry rather than a server-side heartbeat task.

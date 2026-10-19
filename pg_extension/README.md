# Opsqueue PostgreSQL extension

This crate adds a PostgreSQL-native queue API alongside the existing Opsqueue service. PostgreSQL stores submission state, chunk payloads, retry history, and reservation leases. Consumers claim work using `FOR UPDATE SKIP LOCKED`; expired leases become claimable again.

The extension currently targets PostgreSQL 16 through `pgrx` 0.19.3. It does not replace the HTTP/WebSocket service or migrate the Rust/Python clients yet. Payloads can be stored directly in PostgreSQL as `bytea`, or represented by a `prefix` and fetched from an external object store by the client.

## Build and install

Install `cargo-pgrx` 0.19.3 and initialize a user-owned PostgreSQL 16 instance:

```bash
cargo pgrx init --pg16 download
```

Run this as your normal user, not with `sudo`. Without a version flag, `cargo pgrx init` builds all of pgrx's default PostgreSQL versions.

Then, from this directory, build and start a pgrx-managed PostgreSQL instance:

```bash
cargo pgrx run pg16
```

In the launched database, create the extension:

```sql
CREATE EXTENSION opsqueue_pg;
```

The extension creates the `opsqueue` schema and installs its objects there.

## Tests

Run the PostgreSQL-backed integration tests from this directory with:

```bash
cargo pgrx test pg16
```

The suite exercises submission creation, oldest/newest/random and prefer-distinct dispatch, exclusive reservation and renewal, completion, retries and expired leases, pause/cancel transitions, state counters, metadata filtering, and retention cleanup.

## SQL API

- `opsqueue.submit(...)` inserts a submission and optionally its `bytea[]` chunks. Use `p_initial_status => 'paused'` to hold work until explicitly unpaused.
- `opsqueue.reserve(worker_uuid, limit, lease, strategy, prefer_distinct_keys, underlying_strategy, max_retries)` atomically claims chunks and returns their payload, submission metadata, and trace context. The default lease is 10 minutes. Strategies are `oldest`, `newest`, `random`, and `prefer_distinct`.
- `opsqueue.renew(...)` extends a worker's unexpired lease. `opsqueue.reap_expired(max_retries, limit)` records expired claims as failed attempts; `reserve` invokes it before claiming work.
- `opsqueue.complete(...)` stores a result and marks the submission completed when its final chunk completes.
- `opsqueue.fail(...)` records a retry and returns the chunk to the queue, or fails the submission after `p_max_retries`.
- `opsqueue.status(id)` includes the current pending chunk count as `chunks_ready`. `opsqueue.results(id)`, `opsqueue.failure_history(id)`, `lookup_by_prefix(prefix)`, and `lookup_by_metadata(jsonb, max_results)` provide producer-side queries. Metadata lookup is limited to in-progress submissions and defaults to a 100,000-result cap; exceeding the cap raises an error.
- `opsqueue.count_in_progress()`, `count_completed()`, `count_failed()`, `count_paused()`, and `count_cancelled()` report submission counts by state.
- `opsqueue.count_chunks()`, `count_chunks_completed()`, and `count_chunks_failed()` report dispatchable, completed, and permanently failed chunk counts.
- `opsqueue.cancel(id)` cancels an in-progress or paused submission; `opsqueue.unpause(id)` makes paused work available.
- `opsqueue.cleanup(older_than, limit)` deletes a bounded batch of completed, failed, and cancelled submissions older than the cutoff; their chunks and retry history are removed by foreign-key cascades. Repeat it from a PostgreSQL scheduler or maintenance job until it returns zero.

Each consumer must use a stable, unique UUID as its worker ID and renew leases for work that may exceed the lease interval. A lost connection is recovered by lease expiry rather than a server-side heartbeat task.

Consumers can subscribe to `LISTEN opsqueue_work_available` and call `reserve` after receiving a notification. Notifications are delivered at transaction commit; the payload is the submission ID. They are wakeup hints, not a durable work log, so consumers should still reserve on startup and after reconnecting.

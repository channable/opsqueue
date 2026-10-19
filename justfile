# Default to strict shell settings
set shell := ["bash", "-euo", "pipefail", "-c"]

# When just tryping `just`, show list of known commands
_default:
  just --list --unsorted

# Timeout (seconds) for Python integration test recipes.
# Override with OPSQUEUE_PYTEST_TIMEOUT, e.g. OPSQUEUE_PYTEST_TIMEOUT=600 just nix-test-integration
pytest_timeout_seconds := env_var_or_default("OPSQUEUE_PYTEST_TIMEOUT", "60")
# The kill window is chosen to be above the `1s` join window on `multiprocess.Process`.
pytest_timeout_kill_seconds := "2s"

# Build-and-run the opsqueue binary (development profile)
[group('run')]
run *OPSQUEUE_ARGS:
  cargo run --bin opsqueue -- {{OPSQUEUE_ARGS}}

# Build the binary and all client libraries (development profile)
[group('build')]
build: build-bin build-python

# Build the opsqueue binary executable (development profile)
[group('build')]
build-bin *ARGS:
  cargo build --bin opsqueue {{ARGS}}

# Build the `opsqueue_python` Python client library (development profile)
[group('build')]
build-python *ARGS:
  #!/usr/bin/env bash
  set -euo pipefail
  cd libs/opsqueue_python
  source "./.setup_local_venv.sh"

  maturin develop {{ARGS}}

[group('build')]
clean:
  cargo clean

# Run all tests
[group('test')]
test: test-doc test-unit test-integration

# Run doctests
# Nextest doesn't currently support doctests
# c.f. https://nexte.st/docs/integrations/test-coverage/
[group('test')]
test-doc *TEST_ARGS:
  cargo test --doc --workspace {{TEST_ARGS}}

# Rust unit test suite
[group('test')]
test-unit *TEST_ARGS:
  cargo nextest run --workspace {{TEST_ARGS}}

# Python integration test suite. Args are forwarded to pytest
[group('test')]
test-integration *TEST_ARGS: build
  #!/usr/bin/env bash
  set -euo pipefail
  export OPSQUEUE_BIN="$PWD/target/debug/opsqueue"

  cd libs/opsqueue_python
  source "./.setup_local_venv.sh"

  timeout --signal term --kill-after {{pytest_timeout_kill_seconds}} {{pytest_timeout_seconds}} pytest --color=yes {{TEST_ARGS}}

# PostgreSQL Python integration tests using a temporary pgrx-managed database
[group('test')]
test-postgres-python *TEST_ARGS: build-python
  #!/usr/bin/env bash
  set -euo pipefail

  cd pg_extension
  pg_config="$(cargo pgrx info pg-config pg16)"
  pg_bin_dir="$(dirname "$pg_config")"
  export PATH="$pg_bin_dir:$PATH"
  cargo pgrx install --pg-config "$pg_config"
  pg_port="${OPSQUEUE_PG16_PORT:-28816}"
  if ! cargo pgrx start pg16; then
    echo "pgrx start returned an error; waiting for PostgreSQL readiness" >&2
  fi
  for ((attempt = 0; attempt < 30; attempt++)); do
    if pg_isready --quiet --host localhost --port "$pg_port"; then
      break
    fi
    if ((attempt == 29)); then
      cargo pgrx status pg16
      echo "PostgreSQL 16 did not become ready on port $pg_port" >&2
      exit 1
    fi
    sleep 1
  done

  test_db="opsqueue_python_test_$$"
  createdb --host localhost --port "$pg_port" "$test_db"
  cleanup() {
    dropdb --if-exists --host localhost --port "$pg_port" "$test_db"
  }
  trap cleanup EXIT

  psql --host localhost --port "$pg_port" --dbname "$test_db" \
    --set ON_ERROR_STOP=1 --command 'CREATE EXTENSION opsqueue_pg'

  export OPSQUEUE_TEST_POSTGRES_DSN="host=localhost port=$pg_port dbname=$test_db"
  cd ../libs/opsqueue_python
  source ./.setup_local_venv.sh
  timeout --signal term --kill-after {{pytest_timeout_kill_seconds}} {{pytest_timeout_seconds}} \
    pytest --color=yes -n0 tests/test_postgres_clients.py {{TEST_ARGS}}

# Python integration test suite, using artefacts built through Nix. Args are forwarded to pytest
[group('nix')]
nix-test-integration *TEST_ARGS: nix-build
  #!/usr/bin/env bash
  set -euo pipefail
  nix_build_python_library_dir=$(just nix-build-python)
  nix_build_bin_dir=$(just nix-build-bin)

  cd libs/opsqueue_python/tests
  export PYTHONPATH="${nix_build_python_library_dir}/lib/python3.13/site-packages"
  export OPSQUEUE_BIN="${nix_build_bin_dir}/bin/opsqueue"
  export RUST_LOG="opsqueue=debug"

  timeout --signal term --kill-after {{pytest_timeout_kill_seconds}} {{pytest_timeout_seconds}} pytest --color=yes {{TEST_ARGS}}

# Benchmark query cost and render a graph.
[group('bench')]
bench-chunks-select:
  #!/usr/bin/env bash
  set -euo pipefail
  cargo bench --bench chunks_select

  source "./libs/opsqueue_python/.setup_local_venv.sh"
  python opsqueue/benches/plot_chunks_select.py

# Run all linters, fast and slow
[group('lint')]
lint: (lint-light "--show-diff-on-failure" "--all-files") lint-heavy

# Run only the fast per-file linters; these might opt to only look at the changed files. Args are passed to pre-commit
[group('lint')]
lint-light *ARGS:
  pre-commit run {{ARGS}}

# Run the slow linters/static analysers that need to look at everything
[group('lint')]
[parallel]
lint-heavy: semver clippy-fix lint-cargo mypy

# Verify the semver bounds are respected since the last tagged build
[group('lint')]
semver:
  #!/usr/bin/env bash
  set -euo pipefail
  export DATABASE_URL="sqlite://{{justfile_directory()}}/opsqueue/opsqueue_example_database_schema.db"
  # We select the latest git tag, not to be confused with the latest Cargo version.
  cargo semver-checks --workspace --target x86_64-unknown-linux-gnu --baseline-rev "$(git tag -l --sort=-version:refname | head -1)" || {
    echo "Semver checks failed. Please bump the version in Cargo.toml"
    exit 1
  }

# Rust static analysis
[group('lint')]
clippy-fix:
  # `cargo clippy --fix` caps lints to warnings internally; keep its artifacts separate.
  CARGO_TARGET_DIR=target/clippy-fix cargo clippy --no-deps --all-targets --fix --allow-dirty --allow-staged -- -Dwarnings
  CARGO_TARGET_DIR=target/clippy-fix cargo clippy --no-deps --all-targets --no-default-features --fix --allow-dirty --allow-staged -- -Dwarnings
  CARGO_TARGET_DIR=target/clippy-fix cargo clippy --no-deps --all-targets --all-features --fix --allow-dirty --allow-staged -- -Dwarnings

# Serial execution on the main `CARGO_TARGET_DIR`
[group('lint')]
lint-cargo: clippy hakari

# Verify the workspace-hack crate is up to date
[group('lint')]
hakari:
  cargo hakari verify

# Rust static analysis
[group('lint')]
clippy:
  cargo clippy --no-deps --all-targets -- -Dwarnings
  cargo clippy --no-deps --all-targets --no-default-features -- -Dwarnings
  cargo clippy --no-deps --all-targets --all-features -- -Dwarnings

# Python static analysis type-checker
[group('lint')]
mypy:
  dmypy --version
  dmypy run -- --strict --follow-imports=normal --junit-xml="" ./libs/opsqueue_python

# Build Nix-derivations of binary and all libraries (release profile)
[group('nix')]
nix-build: (_nix-build "opsqueue" "python.pkgs.opsqueue_python")

# Build Nix-derivation of binary (release profile)
[group('nix')]
nix-build-bin: (_nix-build "opsqueue")

# Build Nix-derivation of Python client library (release profile)
[group('nix')]
nix-build-python: (_nix-build "python.pkgs.opsqueue_python")

_nix-build +TARGETS:
  nix build --file nix/nixpkgs-pinned.nix --print-out-paths --print-build-logs --no-link --option sandbox true {{TARGETS}}

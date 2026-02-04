# When just tryping `just`, show list of known commands
_default:
  just --list --unsorted

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
test: test-unit test-integration

# Rust unit test suite
[group('test')]
test-unit *TEST_ARGS:
  cargo nextest run {{TEST_ARGS}}

# Python integration test suite. Args are forwarded to pytest
[group('test')]
test-integration *TEST_ARGS: build-bin build-python
  #!/usr/bin/env bash
  set -euo pipefail
  cd libs/opsqueue_python
  source "./.setup_local_venv.sh"

  pytest --color=yes {{TEST_ARGS}}

# Python integration test suite, using artefacts built through Nix. Args are forwarded to pytest
[group('nix')]
nix-test-integration *TEST_ARGS: nix-build-bin
  #!/usr/bin/env bash
  set -euo pipefail
  nix_build_python_library_dir=$(just nix-build-python)

  cd libs/opsqueue_python/tests
  export PYTHONPATH="${nix_build_python_library_dir}/lib/python3.13/site-packages"
  export OPSQUEUE_VIA_NIX=true
  export RUST_LOG="opsqueue=debug"

  pytest --color=yes {{TEST_ARGS}}

# Run all linters, fast and slow
[group('lint')]
lint: lint-light lint-heavy

# Run only the fast per-file linters; these might opt to only look at the changed files. Args are passed to pre-commit
[group('lint')]
lint-light *ARGS:
  pre-commit run {{ARGS}}

# Run the slow linters/static analysers that need to look at everything
[group('lint')]
lint-heavy: clippy mypy

# Rust static analysis
[group('lint')]
clippy:
  cargo clippy --no-deps --fix --allow-dirty --allow-staged -- -Dwarnings

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

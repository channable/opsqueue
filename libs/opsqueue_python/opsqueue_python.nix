{
  # Builtin
  pkgs,
  lib,

  # Rust version. (Override this with an overlay if you like)
  rustToolchain,

  # Native build dependencies:
  maturin,
  buildPythonPackage,
  perl,
  git,

  # Python package dependencies:
  cbor2,
  opentelemetry-api,
  opentelemetry-exporter-otlp,
  opentelemetry-sdk,
}:
let
  python = pkgs.python3;
  sources = import ../../nix/sources.nix;
  crane = import sources.crane { pkgs = pkgs; };
  craneLib = crane.overrideToolchain (pkgs: rustToolchain);

  # Only the files necessary to build the Rust-side and cache dependencies
  sqlFileFilter = path: _type: builtins.match "^.*\.(db|sql)$" path != null;
  rustCrateFileFilter =
    path: type: (sqlFileFilter path type) || (craneLib.filterCargoSources path type);
  depsSrc = lib.cleanSourceWith {
    src = ../../.;
    name = "opsqueue";
    filter = rustCrateFileFilter;
  };

  # Full set of files to build the Python wheel on top
  pythonFileFilter = path: _type: builtins.match "^.*\.(py|md)$" path != null;
  wheelFileFilter = path: type: (pythonFileFilter path type) || (rustCrateFileFilter path type);
  wheelSrc = lib.cleanSourceWith {
    src = ../../.;
    name = "opsqueue";
    filter = wheelFileFilter;
  };

  crateName = craneLib.crateNameFromCargoToml { cargoToml = ./Cargo.toml; };
  pname = crateName.pname;
  version = (craneLib.crateNameFromCargoToml { cargoToml = ../../Cargo.toml; }).version;
  commonArgs = {
    inherit version pname;
    src = depsSrc;
    strictDeps = true;
    nativeBuildInputs = [ python ];
    cargoExtraArgs = "--package opsqueue_python";
    doCheck = false;
  };
  cargoArtifacts = craneLib.buildDepsOnly (commonArgs // { pname = pname; });

  wheelTail = "py3-abi3-linux_x86_64";
  wheelName = "opsqueue-${version}-${wheelTail}.whl";

  crateWheel =
    (craneLib.buildPackage (
      commonArgs
      // {
        inherit cargoArtifacts;
        src = wheelSrc;
      }
    )).overrideAttrs
      (old: {
        nativeBuildInputs = old.nativeBuildInputs ++ [ maturin ];
        env.PYO3_PYTHON = python.interpreter;

        # We intentionally _override_ rather than extend the buildPhase
        # as Maturin itself calls `cargo build`, no need to call it twice.
        buildPhase = ''
          cargo --version
          maturin build --release --offline --target-dir ./target --manifest-path "./libs/opsqueue_python/Cargo.toml"
        '';

        # We build a single wheel
        # but by convention its name is based on the precise combination of
        # Python version + OS version + architecture + ...
        #
        # The Nix hash already covers us for uniqueness and compatibility.
        # So this 'trick' copies it to a predictably named file.
        #
        # Just like `buildPhase`, we override rather than extend
        # because we are only interested in the wheel output of Maturin as a whole.
        # (which is an archive inside of it containing the `.so` cargo built)
        installPhase = ''
          mkdir -p $out
          for wheel in ./target/wheels/*.whl ; do
            cp "$wheel" $out/${wheelName}
          done
        '';

        # There are no Rust unit tests in the Python FFI library currently,
        # so we can skip rebuilding opsqueue_python for tests.
        doCheck = false;
      });
in
buildPythonPackage {
  pname = pname;
  format = "wheel";
  version = version;
  src = "${crateWheel}/${wheelName}";
  doCheck = false;
  pythonImportsCheck = [
    "opsqueue"
    # Internal: This is the most important one!
    "opsqueue.opsqueue_internal"
    # Visible:
    "opsqueue.producer"
    "opsqueue.consumer"
    "opsqueue.exceptions"
    "opsqueue.common"
  ];

  propagatedBuildInputs = [
    cbor2
    opentelemetry-api
    opentelemetry-exporter-otlp
    opentelemetry-sdk
  ];
}

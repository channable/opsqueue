{
  lib,
  buildPythonPackage,
  rustPlatform,
  perl,
  git,
  # Python packages:
  cbor2,
  opentelemetry-api,
  opentelemetry-exporter-otlp,
  opentelemetry-sdk,
  opnieuw,
  psutil,
}:
let
  root = ../../.;
  util = import (root + /nix/util.nix) { inherit lib; };
in
buildPythonPackage rec {
  pname = "opsqueue";
  version = "0.1.0";
  pyproject = true;

  src = util.fileFilter {
    name = "opsqueue_python";
    src = root;

    # We're copying slightly too much to the Nix store here,
    # but using the more granular file filter was very error-prone.
    # This is one thing that could be improved a little in the future.
    srcGlobalWhitelist = [
      ".py"
      ".pyi"
      "py.typed"
      ".rs"
      ".toml"
      ".lock"
      ".db"
      ".md"
    ];
  };

  cargoDeps = rustPlatform.importCargoLock { lockFile = ../../Cargo.lock; };

  env = {
    DATABASE_URL = "sqlite://./opsqueue/opsqueue_example_database_schema.db";
  };

  pythonImportsCheck = [ pname ];

  maturinBuildFlags = [
    "--manifest-path"
    "./libs/opsqueue_python/Cargo.toml"
  ];

  nativeBuildInputs = with rustPlatform; [
    perl
    git
    cargoSetupHook
    maturinBuildHook
  ];

  propagatedBuildInputs = [
    cbor2
    opentelemetry-api
    opentelemetry-exporter-otlp
    opentelemetry-sdk
    opnieuw
    psutil
  ];
}

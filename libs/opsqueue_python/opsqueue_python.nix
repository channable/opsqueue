{
  fileFilter,
  buildPythonPackage,
  rustPlatform,
  perl,
  git,
  # Python packages:
  cbor2,
  opentelemetry-api,
  opentelemetry-exporter-otlp,
  opentelemetry-sdk,
}:
buildPythonPackage rec {
  pname = "opsqueue";
  version = "0.1.0";
  pyproject = true;

  src = fileFilter {
    name = "opsqueue_python";
    src = ../../.;

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
    ];
  };

  cargoDeps = rustPlatform.importCargoLock { lockFile = ../../Cargo.lock; };

  env = {
    DATABASE_URL = "sqlite:///build/opsqueue_python/opsqueue/opsqueue_example_database_schema.db";
  };

  pythonImportsCheck = [ pname ];

  maturinBuildFlags = [
    "--manifest-path"
    "/build/opsqueue_python/libs/opsqueue_python/Cargo.toml"
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
  ];
}

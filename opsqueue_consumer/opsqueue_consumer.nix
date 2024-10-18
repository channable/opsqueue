{
  channalib,
  buildPythonPackage,
  rustPlatform,
  perl,
}:

buildPythonPackage rec {
  pname = "opsqueue_consumer";
  version = "0.1.0";
  pyproject = true;

  src = channalib.fileFilter {
    name = "opsqueue_consumer";
    src = ../.;

    srcWhitelist = [
      "Cargo.toml"
      "Cargo.lock"
      "opsqueue_consumer"
      "opsqueue_consumer/Cargo.toml"
      "opsqueue_consumer/src(/.*)?"
      "opsqueue_consumer/pyproject.toml"

      # Opsqueue is a dependency, so that needs to be included too
      "opsqueue"
      "opsqueue/Cargo.toml"
      "opsqueue/.cargo(/.*)?"
      "opsqueue/build\.rs"
      "opsqueue/opsqueue_example_database_schema\.db"
      "opsqueue/app(/.*)?"
      "opsqueue/migrations(/.*)?"
      "opsqueue/src(/.*)?"

      "opsqueue_producer"
      "opsqueue_producer/Cargo.toml"
      "opsqueue_producer/src(/.*)?"
      "opsqueue_producer/pyproject.toml"
    ];

    srcGlobalWhitelist = [
      ".db"
      ".lock"
      ".pyi"
      ".rs"
      ".toml"
      "py.typed"
    ];
  };

  cargoDeps = rustPlatform.importCargoLock {
    lockFile = ../Cargo.lock;
  };

  # preBuild = "${tree}/bin/tree /build/opsqueue_consumer";

  env = {
    DATABASE_URL = "sqlite:///build/opsqueue_consumer/opsqueue/opsqueue_example_database_schema.db";
  };

  pythonImportsCheck = [ pname ];

  maturinBuildFlags = [
    "--manifest-path"
    "opsqueue_consumer/Cargo.toml"
  ];

  nativeBuildInputs = with rustPlatform; [
    perl
    cargoSetupHook
    maturinBuildHook
  ];
}

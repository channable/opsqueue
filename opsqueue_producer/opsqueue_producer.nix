{
  channalib,
  buildPythonPackage,
  rustPlatform,
  perl,
}:

buildPythonPackage rec {
  pname = "opsqueue_producer";
  version = "0.1.0";
  pyproject = true;

  src = channalib.fileFilter {
    name = "opsqueue_producer";
    src = ../.;

    srcWhitelist = [
      "Cargo.toml"
      "Cargo.lock"
      "opsqueue_producer"
      "opsqueue_producer/Cargo.toml"
      "opsqueue_producer/src(/.*)?"
      "opsqueue_producer/pyproject.toml"

      # Opsqueue is a dependency, so that needs to be included too
      "opsqueue"
      "opsqueue/Cargo.toml"
      "opsqueue/.cargo(/.*)?"
      "opsqueue/build\.rs"
      "opsqueue/opsqueue_example_database_schema\.db"
      "opsqueue/app(/.*)?"
      "opsqueue/migrations(/.*)?"
      "opsqueue/src(/.*)?"

      "opsqueue_consumer"
      "opsqueue_consumer/Cargo.toml"
      "opsqueue_consumer/src(/.*)?"
      "opsqueue_consumer/pyproject.toml"
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

  cargoDeps = rustPlatform.importCargoLock { lockFile = ../Cargo.lock; };

  env = {
    DATABASE_URL = "sqlite:///build/opsqueue_producer/opsqueue/opsqueue_example_database_schema.db";
  };

  pythonImportsCheck = [ pname ];

  maturinBuildFlags = [
    "--manifest-path"
    "opsqueue_producer/Cargo.toml"
  ];

  nativeBuildInputs = with rustPlatform; [
    perl
    cargoSetupHook
    maturinBuildHook
  ];
}

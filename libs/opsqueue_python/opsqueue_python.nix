{
  channalib,
  buildPythonPackage,
  rustPlatform,
  perl,
}:

buildPythonPackage rec {
  pname = "opsqueue_python";
  version = "0.1.0";
  pyproject = true;

  src = channalib.fileFilter {
    name = "opsqueue_python";
    src = ../../.;

    srcWhitelist = [
      "Cargo.toml"
      "Cargo.lock"
      "libs/opsqueue_python"
      "libs/opsqueue_python/Cargo.toml"
      "libs/opsqueue_python/src(/.*)?"
      "libs/opsqueue_python/python(/.*)?"
      "libs/opsqueue_python/pyproject.toml"

      # Opsqueue is a dependency, so that needs to be included too
      "opsqueue"
      "opsqueue/Cargo.toml"
      "opsqueue/.cargo(/.*)?"
      "opsqueue/build\.rs"
      "opsqueue/opsqueue_example_database_schema\.db"
      # "opsqueue/app(/.*)?"
      "opsqueue/migrations(/.*)?"
      "opsqueue/src(/.*)?"
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

  cargoDeps = rustPlatform.importCargoLock { lockFile = ../../Cargo.lock; };

  env = {
    DATABASE_URL = "sqlite:///build/opsqueue_python/opsqueue/opsqueue_example_database_schema.db";
  };

  pythonImportsCheck = [ pname ];

  maturinBuildFlags = [
    "--manifest-path"
    "libs/opsqueue_python/Cargo.toml"
  ];

  nativeBuildInputs = with rustPlatform; [
    perl
    cargoSetupHook
    maturinBuildHook
  ];
}

{
  channalib,
  buildPythonPackage,
  rustPlatform,
  perl,
}:

buildPythonPackage rec {
  pname = "opsqueue";
  version = "0.1.0";
  pyproject = true;

  src = channalib.fileFilter {
    name = "opsqueue_python";
    src = ../../.;

    # TODO: I couldn't get it to work with the filter enabled.
    # We're now copying slightly too much to the Nix store.
    # Re-enable properly.

    # srcWhitelist = [
    #   "Cargo.toml"
    #   "Cargo.lock"
    #   "libs/opsqueue_python"
    #   "libs/opsqueue_python/Cargo.toml"
    #   "libs/opsqueue_python/pyproject.toml"
    #   "libs/opsqueue_python/src(/.*)?"
    #   "libs/opsqueue_python/python(/.*)?"

    #   # Opsqueue is a dependency, so that needs to be included too
    #   "opsqueue"
    #   "opsqueue/Cargo.toml"
    #   "opsqueue/.cargo(/.*)?"
    #   "opsqueue/build\.rs"
    #   "opsqueue/opsqueue_example_database_schema\.db"
    #   # "opsqueue/app(/.*)?"
    #   "opsqueue/migrations(/.*)?"
    #   "opsqueue/src(/.*)?"
    # ];

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
    cargoSetupHook
    maturinBuildHook
  ];
}

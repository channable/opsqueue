{
  fileFilter,
  pkgs,
  rustPlatform,
  # Building options
  buildType ? "release",
  # Testing options
  checkType ? "debug",
  doCheck ? true,
  useNextest ? false, # Disabled for now. Re-enable as part of https://github.com/channable/opsqueue/issues/81
  perl,
  git,
}:
rustPlatform.buildRustPackage {
  name = "opsqueue";
  inherit
    buildType
    checkType
    doCheck
    useNextest
    ;

  src = fileFilter {
    name = "opsqueue";
    src = ./.;

    srcWhitelist = [
      "Cargo.toml"
      ".cargo(/.*)?"
      "build\.rs"
      "opsqueue_example_database_schema\.db"
      "app(/.*)?"
      "migrations(/.*)?"
      "src(/.*)?"
    ];

    srcGlobalWhitelist = [
      ".lock"
      ".toml"
      ".rs"
      ".db"
      ".sql"
    ];
  };

  postUnpack = ''
    cp "${../Cargo.lock}" "/build/opsqueue/Cargo.lock"
    chmod +w /build/opsqueue/Cargo.lock
  '';

  env = {
    DATABASE_URL = "sqlite:///build/opsqueue/opsqueue_example_database_schema.db";
  };

  nativeBuildInputs = [
    perl
    git
  ];

  cargoLock = {
    lockFile = ../Cargo.lock;
  };

  # This limits the build to only build the opsqueue executable
  cargoBuildFlags = [
    "--package"
    "opsqueue"
  ];
}

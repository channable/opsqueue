{
  pkgs,
  rustPlatform,
  # Building options
  buildType ? "release",
  # Testing options
  checkType ? "release", # By this point we have it built, so just use it
  doCheck ? true,
  useNextest ? true,
  perl,
}:
rustPlatform.buildRustPackage {
  name = "opsqueue";
  version = "0.1.0";
  inherit
    buildType
    checkType
    doCheck
    useNextest
    ;

  src = pkgs.channalib.fileFilter {
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

  nativeBuildInputs = [ perl ];

  cargoLock = {
    lockFile = ../Cargo.lock;
  };

  # This limits the build to only build the opsqueue executable
  cargoBuildFlags = [
    "--package"
    "opsqueue"
  ];
}

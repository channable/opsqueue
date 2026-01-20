{
  lib,
  rustPlatform,
  # Building options
  buildType ? "release",
  # Testing options
  checkType ? "debug",
  doCheck ? true,
  useNextest ? true,
  perl,
  git,
}:
let
  root = ../.;
  util = import (root + /nix/util.nix) { inherit lib; };
in
rustPlatform.buildRustPackage {
  name = "opsqueue";
  inherit
    buildType
    checkType
    doCheck
    useNextest
    ;

  src = util.fileFilter {
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

  # Print Rust and Cargo versions so we are 100% certain we are using the right ones
  configurePhase = ''
    rustc --version
    cargo --version
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

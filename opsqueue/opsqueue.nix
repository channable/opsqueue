{
  pkgs,
  lib,
  git,

  # Downstream users can override which Rust version is used.
  # But this is opt-in. By default we'll use the same version
  # that we use in this repo for development.
  rustToolchain ? (
    (lib.fix (final: pkgs // (import (import ./nix/sources.nix).rust-overlay) final pkgs))
    .rust-bin.fromRustupToolchainFile
      ./rust-toolchain.toml
  ),
}:
let
  sources = import ../nix/sources.nix;
  crane = import sources.crane { pkgs = pkgs; };
  craneLib = crane.overrideToolchain (pkgs: rustToolchain);
  sqlitePkgConfigPath = lib.makeSearchPathOutput "dev" "lib/pkgconfig" [ pkgs.sqlite ];

  # Only the files necessary to build the Rust-side and cache dependencies
  sqlFileFilter = path: _type: builtins.match "^.*\.(db|sql)$" path != null;
  rustCrateFileFilter =
    path: type: (sqlFileFilter path type) || (craneLib.filterCargoSources path type);

  # src = craneLib.cleanCargoSource ../.;
  src = lib.cleanSourceWith {
    src = ../.;
    name = "opsqueue";
    filter = rustCrateFileFilter;
  };

  crateName = craneLib.crateNameFromCargoToml { cargoToml = ./Cargo.toml; };
  pname = crateName.pname;
  version = (craneLib.crateNameFromCargoToml { cargoToml = ../Cargo.toml; }).version;
  # version = crateName.version;
  commonArgs = {
    inherit src version pname;
    strictDeps = true;
    nativeBuildInputs = [ pkgs.pkg-config ];
    buildInputs = [ pkgs.sqlite ];
    cargoExtraArgs = "--package opsqueue";
    doCheck = true;
  };
  # The `cargo build` still catches the same errors as `cargo check`. The `nix build` executes them
  # serially, this is the slowest part on CI. We have other CI linting steps, i.e. `cargo clippy`,
  # that report these errors earlier.
  cargoArtifacts = craneLib.buildDepsOnly (commonArgs // { cargoCheckCommand = "true"; });
in
craneLib.buildPackage (
  commonArgs
  // {
    inherit cargoArtifacts;

    # Needed for the SQLx macros:
    env = {
      DATABASE_URL = "sqlite:///build/opsqueue/opsqueue/opsqueue_example_database_schema.db";
      LIBSQLITE3_SYS_USE_PKG_CONFIG = "${pkgs.sqlite.dev}";
      PKG_CONFIG_PATH = sqlitePkgConfigPath;
    };

  }
)

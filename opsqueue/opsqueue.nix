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
  extraFileFilter = path: _type: builtins.match "^.*\.(db|sql)$" path != null;
  fileFilter = path: type: (extraFileFilter path type) || (craneLib.filterCargoSources path type);

  # src = craneLib.cleanCargoSource ../.;
  src = lib.cleanSourceWith {
    src = ../.;
    name = "opsqueue";
    filter = fileFilter;
  };

  crateName = craneLib.crateNameFromCargoToml { cargoToml = ./Cargo.toml; };
  pname = crateName.pname;
  version = (craneLib.crateNameFromCargoToml { cargoToml = ../Cargo.toml; }).version;
  # version = crateName.version;
  commonArgs = {
    inherit src version pname;
    strictDeps = true;
    nativeBuildInputs = [ ];
    cargoExtraArgs = "--package opsqueue";
    doCheck = true;
  };
  cargoArtifacts = craneLib.buildDepsOnly commonArgs;
in
craneLib.buildPackage (
  commonArgs
  // {
    inherit cargoArtifacts;

    # Needed for the SQLx macros:
    env = {
      DATABASE_URL = "sqlite:///build/opsqueue/opsqueue/opsqueue_example_database_schema.db";
    };

  }
)

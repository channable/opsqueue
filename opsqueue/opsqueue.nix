{
  pkgs,
  lib,
  rustToolchain,
  # Building options
  buildType ? "release",
  # Testing options
  checkType ? "debug",
  doCheck ? true,
  useNextest ? false, # Disabled for now. Re-enable as part of https://github.com/channable/opsqueue/issues/81
  perl,
  git,
  python312,
}:
let
  sources = import ../nix/sources.nix;
  crane = import sources.crane { pkgs = pkgs; };
  craneLib = crane.overrideToolchain (pkgs: rustToolchain);
  extraFileFilter = path: _type: builtins.match ".*(db|sql)$" path != null;
  fileFilter = path: type: (extraFileFilter path type) || (craneLib.filterCargoSources path type);

  # src = craneLib.cleanCargoSource ../.;
  src = lib.cleanSourceWith {
    src = ../.;
    name = "opsqueue";
    filter = fileFilter;
  };

  crateName = craneLib.crateNameFromCargoToml { cargoToml = ../opsqueue/Cargo.toml; };
  pname = crateName.pname;
  version = crateName.version;
  commonArgs = {
    inherit src version pname;
    strictDeps = true;
    nativeBuildInputs = [ python312 ];
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

    cargoExtraArgs = "--package opsqueue";
  }
)

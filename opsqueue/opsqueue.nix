{
  lib,
  # rustPlatform,
  # naersk,
  craneLib,
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
  extraFileFilter = path: _type: builtins.match ".*(db|sql)$" path != null;
  fileFilter = path: type: (extraFileFilter path type) || (craneLib.filterCargoSources path type);

  # src = craneLib.cleanCargoSource ../.;
  src = lib.cleanSourceWith {
    src = ../.;
    name = "opsqueue";
    filter = fileFilter;
  };

  util = import (../nix/util.nix) { inherit lib; };
  # src = util.fileFilter {
  #   name = "opsqueue";
  #   src = ../.;

  #   srcWhitelist = [
  #     "Cargo.toml"
  #     "Cargo.lock"
  #     "opsqueue/Cargo.toml"
  #     "opsqueue/.cargo(/.*)?"
  #     "opsqueue/build\.rs"
  #     "opsqueue/opsqueue_example_database_schema\.db"
  #     "opsqueue/app(/.*)?"
  #     "opsqueue/migrations(/.*)?"
  #     "opsqueue/src(/.*)?"
  #   ];

  #   srcGlobalWhitelist = [
  #     ".lock"
  #     ".toml"
  #     ".rs"
  #     ".db"
  #     ".sql"
  #   ];
  # };

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
# {
#   name = "opsqueue";
# }
# let
#   root = ../.;
#   util = import (root + /nix/util.nix) { inherit lib; };
# in
# craneLib.buildPackage {
#   name = "opsqueue";
#   inherit
#     buildType
#     checkType
#     doCheck
#     useNextest
#     ;

#   # root = util.fileFilter {
#   #   name = "opsqueue";
#   #   src = ../.;
#   #   srcWhitelist = [
#   #     "Cargo.toml"
#   #     "Cargo.lock"
#   #   ];
#   #   srcGlobalWhitelist = [
#   #     ".lock"
#   #     ".toml"
#   #   ];
#   # };

#   src = util.fileFilter {
#     name = "opsqueue";
#     src = ../.;

#     srcWhitelist = [
#       "Cargo.toml"
#       "Cargo.lock"
#       ".cargo(/.*)?"
#       "build\.rs"
#       "opsqueue_example_database_schema\.db"
#       "app(/.*)?"
#       "migrations(/.*)?"
#       "src(/.*)?"
#     ];

#     srcGlobalWhitelist = [
#       ".lock"
#       ".toml"
#       ".rs"
#       ".db"
#       ".sql"
#     ];
#   };

#   # postUnpack = ''
#   #   cp "${../Cargo.lock}" "/build/opsqueue/Cargo.lock"
#   #   chmod +w /build/opsqueue/Cargo.lock
#   # '';

#   env = {
#     DATABASE_URL = "sqlite:///build/opsqueue/opsqueue_example_database_schema.db";
#   };

#   nativeBuildInputs = [
#     perl
#     git
#   ];

#   # cargoLock = {
#   #   lockFile = ../Cargo.lock;
#   # };

#   # This limits the build to only build the opsqueue executable
#   # cargoBuildFlags = [
#   #   "--package"
#   #   "opsqueue"
#   # ];
# }

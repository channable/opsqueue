{
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
let
  git-rev = pkgs.runCommand "gitrev" {
    buildInputs = [ pkgs.git ];
  } "git --git-dir ${./../.git} rev-parse HEAD > $out";
in
rustPlatform.buildRustPackage {
  name = "opsqueue";
  version = git-rev;
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
    GIT_REV_REPORTED_BY_NIX = git-rev;
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

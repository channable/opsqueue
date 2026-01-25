{
  # Builtin
  pkgs,
  lib,

  # Rust version. (Override this with an overlay if you like)
  rustToolchain,

  # Native build dependencies:
  python,
  maturin,
  buildPythonPackage,
  perl,
  git,

  # Python package dependencies:
  cbor2,
  opentelemetry-api,
  opentelemetry-exporter-otlp,
  opentelemetry-sdk,
}:
let
  sources = import ../../nix/sources.nix;
  crane = import sources.crane { pkgs = pkgs; };
  craneLib = crane.overrideToolchain (pkgs: rustToolchain);
  extraFileFilter = path: _type: builtins.match ".*(db|sql|py|md)$" path != null;
  fileFilter = path: type: (extraFileFilter path type) || (craneLib.filterCargoSources path type);

  src = lib.cleanSourceWith {
    src = ../../.;
    name = "opsqueue";
    filter = fileFilter;
  };

  crateName = craneLib.crateNameFromCargoToml { cargoToml = ./Cargo.toml; };
  pname = crateName.pname;
  version = crateName.version;
  commonArgs = {
    inherit src version pname;
    strictDeps = true;
    nativeBuildInputs = [ python ];
    cargoExtraArgs = "--package opsqueue_python";
  };
  cargoArtifacts = craneLib.buildDepsOnly (commonArgs // { pname = pname; });

  wheelTail = "py3-abi3-linux_x86_64";
  wheelName = "opsqueue-${version}-${wheelTail}.whl";

  crateWheel =
    (craneLib.buildPackage (commonArgs // { inherit cargoArtifacts; })).overrideAttrs
      (old: {
        nativeBuildInputs = old.nativeBuildInputs ++ [ maturin ];

        # We intentionally _override_ rather than extend the buildPhase
        # as Maturin itself calls `cargo build`, no need to call it twice.
        buildPhase = ''
          cargo --version
          maturin build --release --offline --target-dir ./target --manifest-path "./libs/opsqueue_python/Cargo.toml"
        '';

        # We build a single wheel
        # but by convention its name is based on the precise combination of
        # Python version + OS version + architecture + ...
        #
        # The Nix hash already covers us for uniqueness and compatibility.
        # So this 'trick' copies it to a predictably named file.
        #
        # Just like `buildPhase`, we override rather than extend
        # because we are only interested in the wheel output of Maturin as a whole.
        # (which is an archive inside of it containing the `.so` cargo built)
        installPhase = ''
          mkdir -p $out
          for wheel in ./target/wheels/*.whl ; do
            cp "$wheel" $out/${wheelName}
          done
        '';

        # There are no Rust unit tests in the Python FFI library currently,
        # so we can skip rebuilding opsqueue_python for tests.
        doCheck = false;
      });
in
buildPythonPackage {
  pname = pname;
  format = "wheel";
  version = crateName.version;
  src = "${crateWheel}/${wheelName}";
  doCheck = false;
  pythonImportsCheck = [ "opsqueue" ];

  propagatedBuildInputs = [
    cbor2
    opentelemetry-api
    opentelemetry-exporter-otlp
    opentelemetry-sdk
  ];
}

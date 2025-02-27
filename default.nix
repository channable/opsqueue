{
  environment ? "default",
}:
let
  pkgs = import ./nix/nixpkgs-pinned.nix { };

  pythonEnv = pkgs.pythonChannable.withPackages (
    p: with p; [
      click
      build_util
      mypy
      uv
      pytest
      pytest-random-order
      pytest-parallel

      # Repeated here so MyPy sees them:
      cbor2
      opentelemetry-api
      opentelemetry-exporter-otlp
      opentelemetry-sdk
    ]
  );

  defaultEnv = pkgs.buildEnv {
    name = "opsqueue-env-default";
    paths = with pkgs; [
      # For linting and formatting
      pre-commit
      pre-commit-env

      # For compiling the Rust parts
      rust-with-lsp

      # Manage nix pins
      niv
      nvd

      # Rust build tools
      cargo-audit
      cargo-edit
      cargo-nextest
      maturin

      # Tool to locally inspect opentelemetry traces
      jaeger
    ];
  };
  environments = {
    default = defaultEnv;
    shell = pkgs.mkShell {
      packages = [
        defaultEnv
        # For the shell, libpython needs to be in the search path.
        pythonEnv
      ];
    };
  };
in
environments."${environment}"

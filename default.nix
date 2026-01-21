{
  environment ? "default",
}:
let
  pkgs = import ./nix/nixpkgs-pinned.nix { };

  pythonEnv = pkgs.pythonChannable.withPackages (
    p: with p; [
      click
      mypy
      uv
      pytest
      pytest-random-order
      pytest-parallel
      pytest-timeout

      # Repeated here so MyPy sees them:
      cbor2
      opentelemetry-api
      opentelemetry-exporter-otlp
      opentelemetry-sdk
    ]
  );

  defaultEnv = pkgs.buildEnv {
    name = "opsqueue-env-default";
    paths = [
      # Command runner
      pkgs.just

      # For linting and formatting
      pkgs.biome
      pkgs.nixfmt-rfc-style
      pkgs.pre-commit
      pkgs.python3Packages.pre-commit-hooks
      pkgs.ruff

      # For compiling the Rust parts
      pkgs.rustToolchain
      pkgs.sqlx-cli

      # Manage nix pins
      pkgs.niv
      pkgs.nvd

      # Rust build tools
      pkgs.cargo-audit
      pkgs.cargo-edit
      pkgs.cargo-nextest
      pkgs.maturin
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

{
  environment ? "default",
}:
let
  pkgs = import ./nix/nixpkgs-pinned.nix { };

  pythonEnv = pkgs.python.withPackages (
    p: with p; [
      click
      mypy
      uv
      pytest
      pytest-xdist
      pytest-timeout
      multiprocess

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
      pkgs.nixfmt
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

      # sqlite3 binary, for easy debugging/introspection
      pkgs.sqlite
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

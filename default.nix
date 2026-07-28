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
      pkgs.typos

      # For compiling the Rust parts
      pkgs.rustToolchain
      pkgs.sqlx-cli

      # Manage nix pins
      pkgs.niv
      pkgs.nvd

      # Rust build tools
      pkgs.cargo-audit
      pkgs.cargo-edit
      pkgs.cargo-insta
      pkgs.cargo-hakari
      pkgs.cargo-nextest
      pkgs.maturin

      # Resolve native sqlite from Nix for libsqlite3-sys
      pkgs.pkg-config
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
      shellHook = ''
        # We need Cargo to re-run if the sqlite3 dev package changes, so we set this env var to point to it.
        export LIBSQLITE3_SYS_USE_PKG_CONFIG="${pkgs.sqlite.dev}"
        # This is needed for the libsqlite3-sys crate to find the correct sqlite3.
        export PKG_CONFIG_PATH="${pkgs.sqlite.dev}/lib/pkgconfig''${PKG_CONFIG_PATH:+:''${PKG_CONFIG_PATH}}"
      '';
    };
  };
in
environments."${environment}"

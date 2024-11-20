{
  environment ? "default",
}:
let
  pkgs = import ./nix/nixpkgs-pinned.nix { };

  # Rust channel based on the selected runtime, this is a feature of the Mozilla overlay
  rustChannel = pkgs.rustChannelOf { rustToolchain = ./rust-toolchain; };
  rust-with-lsp = rustChannel.rust.override { extensions = [ "rust-src" ]; };

  pythonEnv = pkgs.pythonChannable.withPackages (
    p: with p; [
      click
      build_util
      mypy
      uv
    ]
  );

  defaultEnv = pkgs.buildEnv {
    name = "opsqueue-env-default";
    paths = with pkgs; [
      # For linting and formatting
      ruff
      pre-commit
      biome
      nixfmt-rfc-style
      haskellPackages.fix-whitespace

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

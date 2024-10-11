{
  environment ? "default",
}:
let
  pkgs = import ./nix/nixpkgs-pinned.nix {};

  # Rust channel based on the selected runtime, this is a feature of the Mozilla overlay
  rustChannel = pkgs.rustChannelOf { rustToolchain = ./rust-toolchain; };
  rust-with-lsp = rustChannel.rust.override { extensions = [ "rust-src" ]; };

  defaultEnv = pkgs.buildEnv {
    name = "rust-jobs-env-default";
    paths = with pkgs; [
      # For linting and formatting
      ruff
      
      # For compiling the Rust parts
      rust-with-lsp

      # Manage nix pins
      niv
      nvd

      # Rust build tools
      pkgs.cargo-audit
      pkgs.cargo-edit
      pkgs.cargo-nextest
    ];
  };
  environments = {
    default = defaultEnv;
    shell = pkgs.mkShell { packages = [
      defaultEnv
      # For the shell, libpython needs to be in the search path.
      pkgs.python312
    ]; };
  };
in
environments."${environment}"

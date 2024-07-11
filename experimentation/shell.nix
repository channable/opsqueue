{ pkgs ? import ./nixpkgs-pinned.nix {} }:
  pkgs.mkShell rec {
    python = pkgs.python311.withPackages (ps: [
      # ps.google-cloud-storage
      ps.sqlite-utils
    ]);
    buildInputs = with pkgs; [
      sqlite
      python
      black
      mypy
    ];
  }

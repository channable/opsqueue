{ pkgs ? import ./nixpkgs-pinned.nix {} }:
  pkgs.mkShell rec {
    python = pkgs.python311.withPackages (ps: [
      # ps.google-cloud-storage
      ps.sqlite-utils
      ps.fastapi
      ps.uvicorn
      ps.httpx
      ps.pytest
      ps.requests
    ]);
    buildInputs = with pkgs; [
      sqlite
      python
      black
      mypy
    ];
  }

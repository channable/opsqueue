{ pkgs ? import ../../nix/nixpkgs-pinned.nix { } }:
  pkgs.mkShell rec {
    python = pkgs.python314.withPackages (ps: [
      ps.fastapi
      ps.google-cloud-storage
      ps.httpx
      ps.ipython
      ps.pytest
      ps.requests
      ps.sqlite-utils
      ps.types-requests
      ps.uvicorn
    ]);
    buildInputs = with pkgs; [
      sqlite
      python
      black
      mypy
    ];
  }

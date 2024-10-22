# Extend the arguments that users pass to Nixpkgs to ensure that they can still
# add additional overlays.
args@{
  overlays ? [ ],
  ...
}:
let
  sources = import ./sources.nix;

  used_overlays = [
    (import (sources.nixpkgs-mozilla + "/rust-overlay.nix")) # Used for development only
    (import (sources.devops + "/nix/overlay.nix"))
    (import ./overlay.nix)
  ] ++ overlays;

  nixpkgsArgs = args // {
    overlays = used_overlays;
  };
in
import sources.nixpkgs nixpkgsArgs

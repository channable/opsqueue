# Overlay for Nixpkgs which holds all opsqueue related packages.
final: prev:
let
  sources = import ./sources.nix;
  pythonOverlay = import ./python-overlay.nix { inherit sources; };
in
{
  # Placing the sources in the overlay gives all packages access to the sources,
  # and it makes it possible to override them in new overlays.
  sources = if prev ? sources then prev.sources // sources else sources;

  opsqueue = final.callPackage ../opsqueue/opsqueue.nix { };

  # The explicit choice is made not to override `python312`, as this will cause a rebuild of many
  # packages when nixpkgs uses python 3.12 as default python environment.
  # These packages should not be affected, e.g. cachix. This is because of a transitive
  # dependency on the Python packages that we override.
  # In our case cachix > ghc > shpinx > Python libraries.
  pythonChannable = prev.python312.override { packageOverrides = pythonOverlay; };

}

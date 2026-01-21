# Overlay for Nixpkgs which holds all opsqueue related packages.
final: prev:
let
  pythonOverlay = import ./python-overlay.nix;

  # We want to use the same Rust version in Nix
  # as we use when _not_ using Nix.
  #
  # When using opsqueue as part of your own Nix derivations,
  # be sure to use an overlay to set `rustPlatform`
  # to the desired Rust version
  # if you want to use a different version from the default.
  rustToolchain = final.rust-bin.fromRustupToolchainFile ../rust-toolchain.toml;
  rustPlatform = prev.makeRustPlatform {
    rustc = rustToolchain;
    cargo = rustToolchain;
  };
in
{
  rustToolchain = rustToolchain;
  opsqueue = final.callPackage ../opsqueue/opsqueue.nix { rustPlatform = final.rustPlatform; };

  # The explicit choice is made not to override `python312`, as this will cause a rebuild of many
  # packages when nixpkgs uses python 3.12 as default python environment.
  # These packages should not be affected, e.g. cachix. This is because of a transitive
  # dependency on the Python packages that we override.
  # In our case cachix > ghc > shpinx > Python libraries.
  pythonChannable = prev.python312.override { packageOverrides = pythonOverlay; };

}

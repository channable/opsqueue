# Overlay for Nixpkgs which holds all opsqueue related packages.
final: prev:
let

  # This takes the tooling configuration (rust-analyzer, sources, etc.) from the
  # toolchain file. We don't need all of these extra components for building Nix
  # packages, but having these extra components does not affect the runtime
  # closures of the built Nix packages, and since those packages also need to be
  # available on CI anyways we'll only define a single toolchain that be reused
  # for both packages and dev shells.
  rustToolchain = final.rust-bin.fromRustupToolchainFile ../rust-toolchain.toml;

  rustPlatform = final.makeRustPlatform {
    cargo = rustToolchain;
    rustc = rustToolchain;
  };

  pythonOverlay = import ./python-overlay.nix { inherit rustPlatform; };
in
{
  rustToolchain = rustToolchain;
  opsqueue = final.callPackage ../opsqueue/opsqueue.nix { inherit rustPlatform; };

  # The explicit choice is made not to override `python312`, as this will cause a rebuild of many
  # packages when nixpkgs uses python 3.12 as default python environment.
  # These packages should not be affected, e.g. cachix. This is because of a transitive
  # dependency on the Python packages that we override.
  # In our case cachix > ghc > shpinx > Python libraries.
  pythonChannable = prev.python313.override { packageOverrides = pythonOverlay; };

}

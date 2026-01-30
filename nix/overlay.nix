# Overlay for Nixpkgs which holds all opsqueue related packages.
final: prev:
let
  sources = import ./sources.nix;
  pythonOverlay = import ./python-overlay.nix;

  # We want to use the same Rust version in Nix
  # as we use when _not_ using Nix.
  #
  # When using opsqueue as part of your own Nix derivations,
  # be sure to use an overlay to set `rustToolchain`
  # to the desired Rust version
  # if you want to use a different version from the default.
  rustToolchain = final.rust-bin.fromRustupToolchainFile ../rust-toolchain.toml;

  crane = import sources.crane { pkgs = final; };
  craneLib = crane.overrideToolchain (pkgs: rustToolchain);
  python3 = final.python313.override { packageOverrides = pythonOverlay; };
in
{
  inherit rustToolchain python3;
  python = python3;

  opsqueue = final.callPackage ../opsqueue/opsqueue.nix { };

}

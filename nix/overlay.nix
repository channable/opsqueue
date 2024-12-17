# Overlay for Nixpkgs which holds rust-jobs related packages.
#
# Serves as common overlay for this repository.
#
self: super:
let
  sources = import ./sources.nix;
  pythonOverlay = import ./python-overlay.nix { inherit sources; };
in
{
  # Placing the sources in the overlay gives all packages access to the sources,
  # and it makes it possible to override them in new overlays.
  sources = if super ? sources then super.sources // sources else sources;

  opsqueue = self.callPackage ../opsqueue/opsqueue.nix { };

  # The explicit choice is made not to override `python312`, as this will cause a rebuild of many
  # packages when nixpkgs uses python 3.12 as default python environment.
  # These packages should not be affected, e.g. cachix. This is because of a transitive
  # dependency on the Python packages that we override.
  # In our case cachix > ghc > shpinx > Python libraries.
  pythonOverlay = self.lib.composeExtensions super.pythonOverlay pythonOverlay;
  pythonChannable = super.python312.override { packageOverrides = self.pythonOverlay; };

  # Rust channel based on the selected runtime, this is a feature of the Mozilla overlay
  rustChannel = super.rustChannelOf { rustToolchain = ../rust-toolchain; };
  rust-with-lsp = self.rustChannel.rust.override { extensions = [ "rust-src" ]; };

  pre-commit-env = self.buildEnv {
    name = "pre-commit-env";
    paths = [
      super.python3Packages.pre-commit-hooks
      super.nixfmt-rfc-style
      super.ruff
      super.biome
      self.rust-with-lsp
    ];
  };
}

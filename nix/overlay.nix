# Overlay for Nixpkgs which holds rust-jobs related packages.
#
# Serves as common overlay for this repository.
#
self: super:
let
  sources = import ./sources.nix;
  lib = super.pkgs.lib;
  pythonOverlay = import ./python-overlay.nix { inherit sources; };
in
{
  # A powerful way of filtering the right files for the src attribute of a derivation.
  fileFilter =
    {
      name,
      # Regex whitelist of files
      srcWhitelist ? [ ".*" ],
      # Blacklist of files and directories anywhere in the repository. Use only
      # with files that can appear in multiple places in the repository.
      srcGlobalBlacklist ? [ ],
      # Global whitelist by suffix, only allow the following file extensions
      srcGlobalWhitelist ? [ ],
      # Global whitelist by regex, only allow the following file extensions
      srcGlobalWhitelistRegex ? [ ],
      src,
    }:
    lib.cleanSourceWith rec {
      inherit name src;
      filter =
        path: type:
        let
          relativePath = lib.removePrefix (toString src + "/") path;
        in
        (builtins.any (r: builtins.match r relativePath != null) srcWhitelist)
        &&
          # Whitelist these files
          (
            (type == "directory")
            || (
              (builtins.length srcGlobalWhitelist != 0)
              && (builtins.any (suffix: lib.hasSuffix suffix relativePath) srcGlobalWhitelist)
              || (
                (builtins.length srcGlobalWhitelistRegex != 0)
                && (builtins.any (r: builtins.match r relativePath != null) srcGlobalWhitelistRegex)
              )
            )
          )
        &&
          # Ignore the files from srcGlobalBlacklist anywhere
          !(builtins.elem (baseNameOf path) srcGlobalBlacklist)
        &&
          # Discard editor files, git repo stuff, and other cruft. This one is taken
          # from Nixpkgs; we don't need to implement it ourselves.
          lib.cleanSourceFilter path type;
    };

  # Placing the sources in the overlay gives all packages access to the sources,
  # and it makes it possible to override them in new overlays.
  sources = if super ? sources then super.sources // sources else sources;

  opsqueue = self.callPackage ../opsqueue/opsqueue.nix { };

  # The explicit choice is made not to override `python312`, as this will cause a rebuild of many
  # packages when nixpkgs uses python 3.12 as default python environment.
  # These packages should not be affected, e.g. cachix. This is because of a transitive
  # dependency on the Python packages that we override.
  # In our case cachix > ghc > shpinx > Python libraries.
  pythonChannable = super.python312.override { packageOverrides = pythonOverlay; };

  # We choose a minimal Rust channel to keep the Nix closure size smaller
  rust-with-lsp = self.rust-bin.stable.latest.minimal.override {
    extensions = [
      "clippy"
      "rustfmt"
    ];
  };

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

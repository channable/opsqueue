# Utility functions to use in nix code
{ lib }:
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
}

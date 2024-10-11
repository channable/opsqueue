{ sources ? import ./sources.nix, pkgs }:
self: super: {
  export-feed = self.callPackage ../export-feed/export-feed.nix { };
}

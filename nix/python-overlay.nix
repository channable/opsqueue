{
  sources ? import ./sources.nix,
}:
self: super: { opsqueue_python = self.callPackage ../libs/opsqueue_python/opsqueue_python.nix { }; }

{
  sources ? import ./sources.nix,
}:
final: prev: {
  opsqueue_python = final.callPackage ../libs/opsqueue_python/opsqueue_python.nix { };
}

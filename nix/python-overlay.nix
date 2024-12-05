{
  sources ? import ./sources.nix,
}:
self: super: {
  opsqueue_python = self.callPackage ../libs/opsqueue_python/opsqueue_python.nix { };

  # Tests fail
  pymdown-extensions = super.pymdown-extensions.overrideAttrs (_oldAttrs: {
    disabledTestPaths = [ "tests/test_extensions/test_pathconverter.py" ];
  });

}

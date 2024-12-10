{
  sources ? import ./sources.nix,
}:
self: super: {
  opsqueue_python = self.callPackage ../libs/opsqueue_python/opsqueue_python.nix { };

  # Tests fail
  pymdown-extensions = super.pymdown-extensions.overrideAttrs (_oldAttrs: {
    disabledTestPaths = [ "tests/test_extensions/test_pathconverter.py" ];
  });

  # Some packages like opentelemetry-proto and dependees
  # currently have strict version bounds on protobuf, not allowing v5 yet.
  # So for the time being, we override the Python packages to use protobuf 4.
  # This should be temporary. With some luck the next `nixpkgs-unstable`
  # will include an upgraded properly building upgraded version
  #
  # Issue to fix it: https://github.com/channable/opsqueue/issues/129
  protobuf = super.protobuf4;
}

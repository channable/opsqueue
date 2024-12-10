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
  protobuf = super.protobuf4;
  # opentelemetry-proto = super.opentelemetry-proto.override (_prev: {
  #   pythonRelaxDeps = [ "protobuf" ];
  #   protobuf = super.protobuf4;
  # });
}

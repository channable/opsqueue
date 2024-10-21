{
  sources ? import ./sources.nix,
}:
self: super: {
  opsqueue_consumer = self.callPackage ../opsqueue_consumer/opsqueue_consumer.nix { };
  opsqueue_producer = self.callPackage ../opsqueue_producer/opsqueue_producer.nix { };

  # Tests fail
  pymdown-extensions = super.pymdown-extensions.overrideAttrs (_oldAttrs: {
    disabledTestPaths = [ "tests/test_extensions/test_pathconverter.py" ];
  });
}

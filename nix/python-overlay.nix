{ sources ? import ./sources.nix, pkgs }:
self: super: {
  opsqueue_consumer = self.callPackage ../opsqueue_consumer/opsqueue_consumer.nix { };
  opsqueue_producer = self.callPackage ../opsqueue_producer/opsqueue_producer.nix { };
}

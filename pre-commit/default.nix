let
  pkgs = import ../nix/nixpkgs-pinned.nix { };

  defaultEnv = pkgs.buildEnv {
    name = "pre-commit-env";
    paths = with pkgs; [
      # Used to execute the pre-commit hook
      pre-commit
    ];
  };
in
pkgs.mkShell { packages = [ defaultEnv ]; }

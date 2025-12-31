final: prev: {
  opnieuw = final.buildPythonPackage rec {
    pname = "opnieuw";
    version = "3.1.0";
    pyproject = true;

    propagatedBuildInputs = with final; [
      setuptools
      setuptools-scm
    ];

    src = final.fetchPypi {
      inherit pname version;
      sha256 = "sha256-4aLFPQYqnCOBsxdXr41gr5sr/cT9HcN9PLEc+AWwLrY=";
    };
  };

  opsqueue_python = final.callPackage ../libs/opsqueue_python/opsqueue_python.nix { };
}

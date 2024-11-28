# pre-commit

This folder exists to massage [pre-commit] to work with Nix and direnv. Our pre-commit setup works
as follows:

- We install a custom pre-commit hook with the `build.py install pre-commit` command;
- This hook uses `direnv` to cache pre-commit (the tool) and it's dependencies. This in order to
  keep the dependencies up to date automatically but still keep the hook fast;
- Those dependencies are declared in a separate Nix file (`default.nix` in this folder). We use a
  separate Nix file to minimize the dependencies, which means direnv has to reload less often.
- We need a separate `.envrc` file, located in this folder, as one can not specify a file to the
  `direnv exec` command, only a folder.

 [pre-commit]: https://pre-commit.com/

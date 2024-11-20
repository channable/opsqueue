python_version_hash=$(python --version --version | sha1sum | cut -c1-40)

# Then, we (create iff necessary) and activate an (empty!) virtual env
# so Maturin doesn't complain when running `maturin develop`.
if [ ! -d ".venv-$python_version_hash" ]; then
  echo "Creating an empty Python virtualenv to be able to run 'maturin develop', for Python version '$(python --version --version)' (hash $python_version_hash)..."
  python -m venv ".venv-$python_version_hash"
  echo "Done!"
fi

source ".venv-$python_version_hash/bin/activate"

# Ensure `pytest` is available in the venv;
# If we were to use `pytest` from Nix, it would not see the locally built python package!
uv pip install -r pyproject.toml --all-extras --quiet

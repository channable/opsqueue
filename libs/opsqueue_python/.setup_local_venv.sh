relative_dir_path="$(dirname "${BASH_SOURCE[0]}")"
absolute_dir_path="$(cd "$relative_dir_path" && pwd)"

python_version_hash=$(python --version --version | sha1sum | cut -c1-40)
venv_path="$absolute_dir_path/.venv-$python_version_hash"

# Then, we (create iff necessary) and activate an (empty!) virtual env
# so Maturin doesn't complain when running `maturin develop`.
if [ ! -d "$venv_path" ]; then
  echo "Creating an empty Python virtualenv to be able to run 'maturin develop', for Python version '$(python --version --version)' (hash $python_version_hash)..."
  python -m venv "$venv_path"
  echo "Done!"
fi

source "$venv_path/bin/activate"

# Ensure `pytest` is available in the venv;
# If we were to use `pytest` from Nix, it would not see the locally built python package!
uv pip install -r "$absolute_dir_path/pyproject.toml" --all-extras --quiet

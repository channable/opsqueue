# How to run

1. Move to the `opsqueue_consumer` subdirectory. With `direnv`, the extra `.envrc` in those directories will load an (essentially empty) Python virtual environment. This is necessary to make the next step work.

2. Any time you change any Rust code, run [maturin](https://github.com/PyO3/maturin), specifically `maturin develop` to update the Rust<->Python library bindings:
```bash
maturin develop
```

3. Now, just run a Python shell which now (courtesy of the virtual env) has access to the `opsqueue_consumer` module using:
```bash
python
```

# Structure

All logic happens inside the main `opsqueue` crate.
Only the Python-specific parts live inside this library.

You will notice that some structs/enums are defined which seem to be 1:1 copies of definitions inside the main crate.
This is because we cannot add PyO3-specific code, macro calls, conversions, etc. inside the main crate.
And note that this duplication is _fake_ duplication: In cases where we want the Python interface to diverge slightly (or significantly) from the Rust crate's to make it more Python-idiomatic, the types will stop being identical.

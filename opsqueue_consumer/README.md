# How to run

For this library, Nix has not yet been set up.

Instead, the following steps are used:

```bash
# Activate a Python virtualenv in which `maturin` was previously installed using `pip`:
source .env/bin/activate
```

Any time you change any Rust code, run [maturin](https://github.com/PyO3/maturin) to update the Rust<->Python library bindings:
```bash
maturin develop
```

Now, just run a Python shell which now has access to the `opsqueue_producer` module using:
```bash
python
```

# Structure

All logic happens inside the main `opsqueue` crate.
Only the Python-specific parts live inside this library.

You will notice that some structs/enums are defined which seem to be 1:1 copies of definitions inside the main crate.
This is because we cannot add PyO3-specific code, macro calls, conversions, etc. inside the main crate.
And note that this duplication is _fake_ duplication: In cases where we want the Python interface to diverge slightly (or significantly) from the Rust crate's to make it more Python-idiomatic, the types will stop being identical.

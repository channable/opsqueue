Opsqueue is a lightweight batch processing queue system.

This crate describes both the opsqueue queue binary, as well as the core of the various client libraries that can call it.

## Installing the Opsqueue binary

`cargo install opsqueue`

## Using a client library

Currently, we have high-level Python bindings available, c.f. [https://pypi.org/project/opsqueue/](https://pypi.org/project/opsqueue/).

Besides this, the Rust client itself can be used directly, by including `opsqueue` as a library in your project's `Cargo.toml` and enabling the `client-logic` feature-flag:

```toml
opsqueue = {version = "0.30.0", default-features = false, features = ["client-logic"]}
```

## More info

Find full usage instructions and details at the main repository readme:
[https://github.com/channable/opsqueue](https://github.com/channable/opsqueue)

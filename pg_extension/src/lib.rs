use pgrx::prelude::*;

pg_module_magic!();

extension_sql_file!("../sql/install.sql", name = "opsqueue_schema", finalize);

#[pg_extern]
fn opsqueue_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn version_is_available() {
        assert_eq!(crate::opsqueue_version(), env!("CARGO_PKG_VERSION"));
    }
}

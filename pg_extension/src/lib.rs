use pgrx::prelude::*;

pg_module_magic!();

extension_sql_file!("../sql/install.sql", name = "opsqueue_schema", finalize);

#[pg_schema]
pub mod opsqueue {
    use pgrx::prelude::*;

    #[pg_extern]
    pub fn opsqueue_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}

#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&'static str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        Vec::new()
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod pg_tests;

use sqlx::{
    migrate::MigrateDatabase,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
    Connection, Sqlite, SqliteConnection, SqlitePool,
};

pub mod chunk;
pub mod consumer;
pub mod submission;

pub const DATABASE_FILENAME: &str = "opsqueue.db";

pub fn db_options() -> SqliteConnectOptions {
    SqliteConnectOptions::new()
        .filename(DATABASE_FILENAME)
        .journal_mode(SqliteJournalMode::Wal) // This is the default for Sqlx-sqlite and also for Litestream, but good to make sure it is set
        .synchronous(SqliteSynchronous::Normal) // Full is not needed because we use WAL mode
}

pub async fn db_connect_pool() -> SqlitePool {
    SqlitePool::connect_with(db_options())
        .await
        .expect("Could not connect to sqlite DB")
}

pub async fn db_connect_single() -> SqliteConnection {
    SqliteConnection::connect_with(&db_options())
        .await
        .expect("Could not connect to sqlite DB")
}

pub async fn ensure_db_exists() {
    if !Sqlite::database_exists(DATABASE_FILENAME)
        .await
        .unwrap_or(false)
    {
        println!("Creating backing sqlite DB {}", DATABASE_FILENAME);
        Sqlite::create_database(DATABASE_FILENAME)
            .await
            .expect("Could not create backing sqlite DB");
        println!("Finished creating backing sqlite DB");
    } else {
        println!("Starting up using existing sqlite DB {}", DATABASE_FILENAME);
    }
}

pub async fn ensure_db_migrated(db: &SqlitePool) {
    println!("Migrating backing DB");
    sqlx::migrate!("./migrations")
        .run(db)
        .await
        .expect("DB migrations failed");
    println!("Finished migrating backing DB");
}

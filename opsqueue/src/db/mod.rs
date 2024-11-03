use sqlx::{
    migrate::MigrateDatabase,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
    Connection, Sqlite, SqliteConnection, SqlitePool,
};

/// Connects to the SQLite database, 
/// creating it if it doesn't exist yet,
/// and migrating it if it isn't up to date.
/// 
/// This function should be called on app startup; 
/// it will panic when the database cannot be opened or migrated. 
pub async fn open_and_setup(database_filename: &str) -> SqlitePool {
    ensure_db_exists(database_filename).await;
    let db_pool = db_connect_pool(database_filename).await;
    ensure_db_migrated(&db_pool).await;
    db_pool
}

pub fn db_options(database_filename: &str) -> SqliteConnectOptions {
    SqliteConnectOptions::new()
        .filename(database_filename)
        .journal_mode(SqliteJournalMode::Wal) // This is the default for Sqlx-sqlite and also for Litestream, but good to make sure it is set
        .synchronous(SqliteSynchronous::Normal) // Full is not needed because we use WAL mode
}

pub async fn db_connect_pool(database_filename: &str) -> SqlitePool {
    SqlitePool::connect_with(db_options(database_filename))
        .await
        .expect("Could not connect to sqlite DB")
}

pub async fn db_connect_single(database_filename: &str) -> SqliteConnection {
    SqliteConnection::connect_with(&db_options(database_filename))
        .await
        .expect("Could not connect to sqlite DB")
}

pub async fn ensure_db_exists(database_filename: &str) {
    if !Sqlite::database_exists(database_filename)
        .await
        .unwrap_or(false)
    {
        tracing::info!("Creating backing sqlite DB {}", database_filename);
        Sqlite::create_database(database_filename)
            .await
            .expect("Could not create backing sqlite DB");
        tracing::info!("Finished creating backing sqlite DB {}", database_filename);
    } else {
        tracing::info!("Starting up using existing sqlite DB {}", database_filename);
    }
}

pub async fn ensure_db_migrated(db: &SqlitePool) {
    tracing::info!("Migrating backing DB");
    sqlx::migrate!("./migrations")
        .run(db)
        .await
        .expect("DB migrations failed");
    tracing::info!("Finished migrating backing DB");
}

/// We check whether we can not only reach the DB but especially if we can run a transaction.
///
/// This handles the case where for whatever reason some other thing
/// holds the write lock for the DB for a (too) long time.
pub async fn is_db_healthy(pool: &SqlitePool) -> bool {
    async move {
        let mut tx = pool.begin().await?;
        let _count = crate::common::submission::count_submissions(&mut *tx).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await
    .is_ok()
}

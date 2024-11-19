use std::{num::NonZero, time::Duration};

use sqlx::{
    migrate::MigrateDatabase, sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous}, Connection, Sqlite, SqliteConnection, SqlitePool
};

/// We maintain two database connection pools.
///
/// The write pool only contains a single connection,
/// ensuring that any write-contention happens *in async Rust* rather than
/// on a blocking background thread that deals with the blocking SQLite API.
///
/// Conversely, we have _many_ read connections,
/// ensuring that usually there need not be any waiting for any
/// read paths on these.
///
///
/// This also allows us to customize different timeouts
/// and warning thresholds for each pool.
#[derive(Debug, Clone)]
pub struct DBPools {
    pub read_pool: SqlitePool,
    pub write_pool: SqlitePool,
}

/// Connects to the SQLite database,
/// creating it if it doesn't exist yet,
/// and migrating it if it isn't up to date.
///
/// This function should be called on app startup;
/// it will panic when the database cannot be opened or migrated.
pub async fn open_and_setup(database_filename: &str, max_read_pool_size: NonZero<u32>) -> DBPools {
    ensure_db_exists(database_filename).await;
    let read_pool = db_connect_read_pool(database_filename, max_read_pool_size).await;
    let write_pool = db_connect_write_pool(database_filename).await;
    ensure_db_migrated(&write_pool).await;
    DBPools{read_pool, write_pool}
}

pub fn db_options(database_filename: &str) -> SqliteConnectOptions {
    // These settings are currently based on the rule-of-thumb guide:
    // https://kerkour.com/sqlite-for-servers
    SqliteConnectOptions::new()
        .filename(database_filename)
        .journal_mode(SqliteJournalMode::Wal) // This is the default for Sqlx-sqlite and also for Litestream, but good to make sure it is set
        .synchronous(SqliteSynchronous::Normal) // Full is not needed because we use WAL mode
        .busy_timeout(Duration::from_secs(5)) // No query should ever lock for more than 5 seconds
        .foreign_keys(true) // By default SQLite does not do foreign key checks; we want them to ensure data consistency
        .pragma("mmap_size", "134217728")
        .pragma("cache_size", "-1000000") // Cache size of 10⁶ KiB AKA 1GiB (negative value means measured in KiB rather than in multiples of the page size)
        // NOTE: we do _not_ set PRAGMA temp_store = 2 (MEMORY) because as long as the page cache has room those will use memory anyway (and if it is full we need the disk)
}

pub async fn db_connect_read_pool(database_filename: &str, max_read_pool_size: NonZero<u32>) -> SqlitePool {
    SqlitePoolOptions::new()
        .min_connections(16)
        .max_connections(max_read_pool_size.into())
        .connect_with(db_options(database_filename))
        .await
        .expect("Could not connect to sqlite DB")
}

pub async fn db_connect_write_pool(database_filename: &str) -> SqlitePool {
    SqlitePoolOptions::new()
    .min_connections(1)
    .max_connections(1)
    .connect_with(db_options(database_filename))
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
        let mut conn = pool.acquire().await?;
        let mut tx = conn.begin().await?;
        let _count = crate::common::submission::db::count_submissions(&mut *tx).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await
    .is_ok()
}

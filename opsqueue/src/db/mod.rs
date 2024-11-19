use std::{future::Future, ops::{Deref, DerefMut}, time::Duration};

use futures::future::BoxFuture;
use sqlx::{
    migrate::MigrateDatabase, sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous}, Connection, Executor, Sqlite, SqliteConnection, SqlitePool
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
pub async fn open_and_setup(database_filename: &str) -> DBPools {
    ensure_db_exists(database_filename).await;
    let read_pool = db_connect_read_pool(database_filename).await;
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

pub async fn db_connect_read_pool(database_filename: &str) -> SqlitePool {
    SqlitePoolOptions::new()
        .min_connections(16)
        .max_connections(1024)
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

/// Extension trait to add extra functionality that is currently missing from SQLx to its `sqlx::SqliteConnection`.
///
/// c.f. https://github.com/launchbadge/sqlx/issues/481
pub(crate) trait SqliteConnectionExt {
    /// Similar to `sqlx::SqliteConnection::begin`, but uses BEGIN IMMEDIATE
    /// to grab a write lock immediately at the start of the transaction
    /// rather than deferring this to when the first write happens.
    fn begin_immediate(&mut self) -> impl Future<Output = sqlx::Result<ImmediateTransaction>>;

    /// Version of `sqlx::SqliteConnection::transaction` that uses `begin_immediate` rather than `sqlx::Connection::begin`.
    ///
    /// Runs the provided callback in a database transaction that acquires a write-lock immediately on transaction start.
    ///
    /// When the transaction returns Ok, the transaction is committed.
    /// When the callback returns an Err, the transaction is rolled back.
    fn immediate_write_transaction<'a, F, R, E>(&'a mut self, callback: F) -> BoxFuture<'a, Result<R, E>>
    where
        for<'c> F: FnOnce(&'c mut ImmediateTransaction<'_>) -> BoxFuture<'c, Result<R, E>>
            + 'a
            + Send
            + Sync,
        Self: Sized,
        R: Send,
        E: From<sqlx::Error> + Send;
}

impl SqliteConnectionExt for SqliteConnection {
    async fn begin_immediate(&mut self) -> sqlx::Result<ImmediateTransaction> {
        let conn = &mut *self;

        conn.execute("BEGIN IMMEDIATE;").await?;

        Ok(ImmediateTransaction {
            conn,
            is_open: true,
        })
    }

    /// Based on the source of Connection::transaction
    /// https://github.com/launchbadge/sqlx/blob/82d332f4b487440b4c2bd5d54a5f17dcc1abc92c/sqlx-core/src/connection.rs#L69
    fn immediate_write_transaction<'a, F, R, E>(&'a mut self, callback: F) -> BoxFuture<'a, Result<R, E>>
    where
        for<'c> F: FnOnce(&'c mut ImmediateTransaction<'_>) -> BoxFuture<'c, Result<R, E>>
            + 'a
            + Send
            + Sync,
        Self: Sized,
        R: Send,
        E: From<sqlx::Error> + Send,
    {
        Box::pin(async move {
            let mut transaction = self.begin_immediate().await?;
            let ret = callback(&mut transaction).await;

            match ret {
                Ok(ret) => {
                    transaction.commit().await?;

                    Ok(ret)
                }
                Err(err) => {
                    transaction.rollback().await?;

                    Err(err)
                }
            }
        })
    }
}

/// Similar to `sqlx::Transaction` but it was started using BEGIN IMMEDIATE.
///
/// Usage of this transaction should end with a call to `commit` or `rollback`.
/// If neither was called when the transaction goes out of scope,
///  then `rollback`  will be called on drop.
pub(crate) struct ImmediateTransaction<'c> {
    conn: &'c mut SqliteConnection,
    /// is the transaction open, or did we already commit/rollback?
    is_open: bool,
}


impl<'c> ImmediateTransaction<'c> {
    pub(crate) async fn commit(mut self) -> sqlx::Result<()> {
        self.conn.execute("COMMIT;").await?;
        // Even if the conn.execute call failed, we consider the transaction 'finished'.
        self.is_open = false;

        Ok(())
    }

    pub(crate) async fn rollback(mut self) -> sqlx::Result<()> {
        self.conn.execute("ROLLBACK;").await?;
        // Even if the conn.execute call failed, we consider the transaction 'finished'.
        self.is_open = false;

        Ok(())
    }
}

impl<'c> Drop for ImmediateTransaction<'c> {
    fn drop(&mut self) {
        if !self.is_open {
            return;
        }
        let rt_handle = tokio::runtime::Handle::current();
        rt_handle.block_on(async {
            let _ = self.conn.execute("ROLLBACK").await;
        });

    }
}
impl<'c> Deref for ImmediateTransaction<'c> {
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        self.conn
    }
}

impl<'c> DerefMut for ImmediateTransaction<'c> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
    }
}

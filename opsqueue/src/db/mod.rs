use std::{marker::PhantomData, num::NonZero, ops::DerefMut, time::Duration};

use futures::future::BoxFuture;
use sqlx::{
    migrate::MigrateDatabase,
    pool::PoolConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    Connection, Sqlite, SqliteConnection, SqlitePool, Transaction,
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
    pub read_pool: Pool<Reader>,
    pub write_pool: Pool<Writer>,
}

#[cfg(test)]
impl DBPools {
    pub(crate) fn from_test_pool(pool: &sqlx::SqlitePool) -> Self {
        DBPools {
            read_pool: Pool::new(pool.clone()),
            write_pool: Pool::new(pool.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Pool<T> {
    pub(crate) inner: SqlitePool,
    _type: PhantomData<T>,
}

impl<T> Pool<T> {
    /// Wrap the [`SqlitePool`] to add a type-level tag identifying it as either a reader or a writer pool.
    pub fn new(pool: SqlitePool) -> Pool<T> {
        Pool {
            inner: pool,
            _type: PhantomData,
        }
    }
    /// Acquire a transactionless connection.
    pub async fn acquire(&self) -> Result<Conn<T, NoTransaction>, sqlx::Error> {
        self.inner.acquire().await.map(|inner| Conn {
            inner: InnerConn::Pool(inner),
            _type: PhantomData,
        })
    }
}

#[derive(Debug)]
pub struct Conn<T, TX = NoTransaction> {
    inner: InnerConn<TX>,
    _type: PhantomData<T>,
}

/// You can only call these functions if not in a transaction.
impl<T> Conn<T, NoTransaction> {
    pub async fn run_tx<'s, O, E, FN>(&'s mut self, f: FN) -> Result<O, E>
    where
        for<'t> FN:
            FnOnce(Conn<T, TxRef<'t, '_>>) -> BoxFuture<'t, Result<O, E>> + Send + Sync + 't,
        O: Send,
        E: From<sqlx::Error> + Send,
    {
        let inner: &mut SqliteConnection = <Conn<T, NoTransaction> as DerefMut>::deref_mut(self);
        inner
            .transaction(move |tx| {
                let conn = Self::from_tx(tx);
                f(conn)
            })
            .await
    }
}

impl<T, TX> Conn<T, TX> {
    fn from_tx<'t, 'c>(tx: &'t mut Transaction<'c, Sqlite>) -> Conn<T, TxRef<'t, 'c>> {
        Conn {
            inner: InnerConn::Tx(tx),
            _type: PhantomData,
        }
    }
}

/// Empty value that can never be populated. Used for the `TX` slot if we're not in a transaction.
#[derive(Debug)]
pub enum NoTransaction {}

#[derive(Debug)]
enum InnerConn<TX> {
    Pool(PoolConnection<Sqlite>),
    Tx(TX),
}

pub type TxRef<'tx, 'conn> = &'tx mut Transaction<'conn, Sqlite>;

impl<T> std::ops::Deref for Conn<T, NoTransaction> {
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            InnerConn::Pool(pool_connection) => pool_connection,
            InnerConn::Tx(_) => panic!("impossible!"),
        }
    }
}

impl<'tx, 'conn, T> std::ops::DerefMut for Conn<T, NoTransaction> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.inner {
            InnerConn::Pool(pool_connection) => pool_connection,
            InnerConn::Tx(_) => panic!("impossible!"),
        }
    }
}

impl<T> std::ops::Deref for Conn<T, TxRef<'_, '_>> {
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            InnerConn::Pool(pool_connection) => pool_connection,
            InnerConn::Tx(transaction) => transaction,
        }
    }
}

impl<T> std::ops::DerefMut for Conn<T, TxRef<'_, '_>> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.inner {
            InnerConn::Pool(pool_connection) => pool_connection,
            InnerConn::Tx(transaction) => transaction,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Reader {}

#[derive(Debug, Clone)]
pub enum Writer {}

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
    DBPools {
        read_pool,
        write_pool,
    }
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

pub async fn db_connect_read_pool(
    database_filename: &str,
    max_read_pool_size: NonZero<u32>,
) -> Pool<Reader> {
    SqlitePoolOptions::new()
        .min_connections(16)
        .max_connections(max_read_pool_size.into())
        .connect_with(db_options(database_filename))
        .await
        .map(Pool::new)
        .expect("Could not connect to sqlite DB")
}

pub async fn db_connect_write_pool(database_filename: &str) -> Pool<Writer> {
    SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .connect_with(db_options(database_filename))
        .await
        .map(Pool::new)
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

pub async fn ensure_db_migrated(db: &Pool<Writer>) {
    tracing::info!("Migrating backing DB");
    sqlx::migrate!("./migrations")
        // When rolling back, we want to be able to keep running even when the DB's schema is newer:
        .set_ignore_missing(true)
        .run(&db.inner)
        .await
        .expect("DB migrations failed");
    tracing::info!("Finished migrating backing DB");
}

/// We check whether we can not only reach the DB but especially if we can run a transaction.
///
/// This handles the case where for whatever reason some other thing
/// holds the write lock for the DB for a (too) long time.
pub async fn is_db_healthy(pool: &Pool<Writer>) -> bool {
    match pool.acquire().await {
        Ok(mut conn) => conn
            .run_tx(|mut tx| {
                Box::pin(async move {
                    let _count = crate::common::submission::db::count_submissions(&mut tx).await?;
                    Ok::<_, anyhow::Error>(())
                })
            })
            .await
            .is_ok(),
        Err(error) => {
            tracing::error!("DB unhealthy; could not acquire DB connection: {error:?}");
            false
        }
    }
}

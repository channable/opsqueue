//! Core database abstractions for the opsqueue server internals.
//!
//! We use compile-time constraints to express requirements for our queries. For this, we
//! have some type-level ✨ magic ✨ to help us do this in a readable way.
//!
//! The core of this system is the [`TypedConnection`] along with a number of type parameters
//! on the [`Conn`] struct.
//!
//! # Example
//!
//! This is how the interface works in practice.
//!
//! ```
//! use opsqueue::db::{TypedConnection, bool::*, Pool};
//!
//! // Let's say that we do some kind of operation here that requires
//! // write access to the database. Note that it's up to the author
//! // of the function to ensure that the correct constraints are added.
//! async fn some_insert(
//!     mut conn: impl TypedConnection<Writer = True>
//! ) -> sqlx::Result<()> {
//!     let n: i32 = sqlx::query_scalar("SELECT 1")
//!         .fetch_one(conn.get_inner())
//!         .await?;
//!     assert_eq!(n, 1);
//!     Ok(())
//! }
//!
//! // This one does not require any write access, so we should leave
//! // that unspecified. This way we can use either a reader or a
//! // writer connection.
//! async fn some_select(
//!     mut conn: impl TypedConnection
//! ) -> sqlx::Result<()> {
//!     let n: i32 = sqlx::query_scalar("SELECT 2")
//!         .fetch_one(conn.get_inner())
//!         .await?;
//!     assert_eq!(n, 2);
//!     Ok(())
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> sqlx::Result<()> {
//! // This is not how you acquire the production database pool. This
//! // is for demonstration only. Use `open_and_setup`.
//! let db = sqlx::SqlitePool::connect(":memory:").await?;
//! let mut conn = Pool::new(db).writer_conn().await?;
//!
//! some_insert(&mut conn).await?;
//! some_select(&mut conn).await?;
//! # Ok (()) }
//! ```

use std::{marker::PhantomData, num::NonZero, time::Duration};

use bool::Bool;
use futures::future::BoxFuture;
use sqlx::{
    migrate::MigrateDatabase,
    pool::PoolConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    Connection, Sqlite, SqliteConnection, SqlitePool, Transaction,
};

pub use bool::{False, True};

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
/// This also allows us to customize different timeouts
/// and warning thresholds for each pool.
#[derive(Debug, Clone)]
pub struct DBPools {
    read_pool: Pool<Reader>,
    write_pool: Pool<Writer>,
}

impl DBPools {
    /// Create a `DBPools` instance from a single test pool. Only usable in tests.
    #[cfg(test)]
    pub(crate) fn from_test_pool(pool: &sqlx::SqlitePool) -> Self {
        DBPools {
            read_pool: Pool::new(pool.clone()),
            write_pool: Pool::new(pool.clone()),
        }
    }
    /// Access the pool containing the reader connections.
    pub fn reader_pool(&self) -> &Pool<Reader> {
        &self.read_pool
    }
    /// Access the pool containing the writer connection.
    pub fn writer_pool(&self) -> &Pool<Writer> {
        &self.write_pool
    }
    /// Access a reader connection.
    ///
    /// Such a connection can't be used to make changes to the state in the database.
    pub async fn reader_conn(&self) -> Result<Conn<Reader, NoTransaction>, sqlx::Error> {
        self.read_pool.acquire().await
    }
    /// Access a writer connection.
    ///
    /// This connection can be used both for functions requiring read-only access and for functions
    /// that make changes to the state in the database.
    pub async fn writer_conn(&self) -> Result<Conn<Writer, NoTransaction>, sqlx::Error> {
        self.write_pool.acquire::<Writer>().await
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
    /// Acquire one of the read-only database connections.
    ///
    /// If you need a `Conn<Writer>`, you need to use `writer_conn` on `Pool<Writer>`.
    ///
    /// See [`DBPools`] for further explanation about readers and writers.
    pub async fn reader_conn(&self) -> Result<Conn<Reader, NoTransaction>, sqlx::Error> {
        self.acquire().await
    }
    /// Acquire a new connection from the underlying pool and wrap it in our typed connection.
    ///
    /// Internal convenience method for implementing `writer_conn` and `reader_conn`-esque inherent methods.
    async fn acquire<X>(&self) -> Result<Conn<X, NoTransaction>, sqlx::Error> {
        self.inner.acquire().await.map(|inner| Conn {
            inner: InnerConn::Pool(inner),
            _type: PhantomData,
        })
    }
}

impl Pool<Writer> {
    /// Acquire the connection capable of writing to the database.
    ///
    /// See [`DBPools`] for further explanation about readers and writers.
    pub async fn writer_conn(&self) -> Result<Conn<Writer, NoTransaction>, sqlx::Error> {
        self.acquire().await
    }
}

#[derive(Debug)]
pub struct Conn<Xs, Tx> {
    inner: InnerConn<Tx>,
    _type: PhantomData<Xs>,
}

pub trait TypedConnection {
    /// Indicates whether this is a writer connection or not. This can be used to constrain the
    /// allowed connections to only connections obtained from the writer pool, so that the user
    /// will be alerted to misuse
    type Writer: Bool;
    type Transaction: Bool;

    /// Access the [`sqlx`] connection.
    fn get_inner(&mut self) -> &mut SqliteConnection;

    #[allow(async_fn_in_trait)]
    async fn run_tx<O, E, F>(&mut self, f: F) -> Result<O, E>
    where
        for<'t> F: FnOnce(
                Conn<<Self::Writer as Bool>::If<Writer, Reader>, TxRef<'t, '_>>,
            ) -> BoxFuture<'t, Result<O, E>>
            + Send
            + Sync
            + 't,
        O: Send,
        E: From<sqlx::Error> + Send,
    {
        self.get_inner()
            .transaction(move |tx| {
                let conn = Conn {
                    inner: InnerConn::InTransaction(tx),
                    _type: PhantomData,
                };
                f(conn)
            })
            .await
    }
}

pub trait WriterConnection: TypedConnection<Writer = True> {
    type Transaction: Bool;
}

impl<T> WriterConnection for T
where
    T: TypedConnection<Writer = True>,
{
    type Transaction = <T as TypedConnection>::Transaction;
}

impl<C> TypedConnection for &mut C
where
    C: TypedConnection,
{
    type Writer = <C as TypedConnection>::Writer;
    type Transaction = <C as TypedConnection>::Transaction;

    fn get_inner(&mut self) -> &mut SqliteConnection {
        <C as TypedConnection>::get_inner(*self)
    }
}

pub mod bool {
    //! Home of the sealed [`Bool`] trait, used for indicating constraints on
    //! a [`TypedConnection`][super::TypedConnection].

    pub enum True {}
    pub enum False {}

    /// A type-level boolean
    pub trait Bool {
        type If<Then, Else>;
    }

    impl Bool for True {
        type If<Then, Else> = Then;
    }
    impl Bool for False {
        type If<Then, Else> = Else;
    }
}

impl TypedConnection for Conn<Writer, NoTransaction>
where
    Self: std::ops::Deref<Target = SqliteConnection> + std::ops::DerefMut,
{
    type Writer = True;
    type Transaction = False;
    fn get_inner(&mut self) -> &mut SqliteConnection {
        &mut *self
    }
}

impl TypedConnection for Conn<Reader, NoTransaction>
where
    Self: std::ops::Deref<Target = SqliteConnection> + std::ops::DerefMut,
{
    type Writer = False;
    type Transaction = False;
    fn get_inner(&mut self) -> &mut SqliteConnection {
        &mut *self
    }
}

impl TypedConnection for Conn<Writer, TxRef<'_, '_>>
where
    Self: std::ops::Deref<Target = SqliteConnection> + std::ops::DerefMut,
{
    type Writer = True;
    type Transaction = True;
    fn get_inner(&mut self) -> &mut SqliteConnection {
        &mut *self
    }
}

impl TypedConnection for Conn<Reader, TxRef<'_, '_>>
where
    Self: std::ops::Deref<Target = SqliteConnection> + std::ops::DerefMut,
{
    type Writer = False;
    type Transaction = True;
    fn get_inner(&mut self) -> &mut SqliteConnection {
        &mut *self
    }
}

/// Empty value that can never be populated. Used for the `TX` slot if we're not in a transaction.
#[derive(Debug)]
pub enum NoTransaction {}

#[derive(Debug)]
enum InnerConn<Tx> {
    Pool(PoolConnection<Sqlite>),
    InTransaction(Tx),
}

/// Represents a reference to a transaction.
pub type TxRef<'tx, 'conn> = &'tx mut Transaction<'conn, Sqlite>;

impl<T> std::ops::Deref for Conn<T, NoTransaction> {
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            InnerConn::Pool(pool_connection) => pool_connection,
            InnerConn::InTransaction(_) => panic!("impossible!"),
        }
    }
}

impl<T> std::ops::DerefMut for Conn<T, NoTransaction> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.inner {
            InnerConn::Pool(pool_connection) => pool_connection,
            InnerConn::InTransaction(_) => panic!("impossible!"),
        }
    }
}

impl<T> std::ops::Deref for Conn<T, TxRef<'_, '_>> {
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            InnerConn::Pool(pool_connection) => pool_connection,
            InnerConn::InTransaction(transaction) => transaction,
        }
    }
}

impl<T> std::ops::DerefMut for Conn<T, TxRef<'_, '_>> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.inner {
            InnerConn::Pool(pool_connection) => pool_connection,
            InnerConn::InTransaction(transaction) => transaction,
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
    match pool.writer_conn().await {
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

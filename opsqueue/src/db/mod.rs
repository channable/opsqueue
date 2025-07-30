//! Core database abstractions for the opsqueue server internals.
//!
//! We use compile-time constraints to express requirements for our queries. For this, we
//! have some type-level ✨ magic ✨ to help us do this in a readable way.
//!
//! The core of this system is the [`Connection`] along with a number of type parameters
//! on the [`Conn`] struct. You connect to the database using [`open_and_setup`].
//!
//! # Example
//!
//! This is how the interface works in practice.
//!
//! ```
//! use opsqueue::db::{Connection, True, WriterPool};
//!
//! // Let's say that we do some kind of operation here that requires
//! // write access to the database. Note that it's up to the author
//! // of the function to ensure that the correct constraints are added.
//! // In this situation, you'd typically use `WriterConnection`, which
//! // is an alias for `Connection<Writable = True>`.
//! async fn some_insert(
//!     mut conn: impl Connection<Writable = True>
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
//!     mut conn: impl Connection
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
//! let mut conn = WriterPool::new(db).writer_conn().await?;
//!
//! some_insert(&mut conn).await?;
//! some_select(&mut conn).await?;
//! # Ok (()) }
//! ```

use std::{marker::PhantomData, num::NonZero, time::Duration};

use futures::future::BoxFuture;
use magic::Bool;
use sqlx::{
    migrate::MigrateDatabase,
    sqlite::SqlitePoolOptions,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
    Connection as _, Sqlite, SqliteConnection, SqlitePool,
};

use conn::{Conn, NoTransaction, Reader, Tx, Writer};

pub use magic::{False, True};

/// A [`Pool`] that can produce [`Writer`]s.
pub type WriterPool = Pool<True>;
/// A [`Pool`] that can produce [`Reader`]s.
pub type ReaderPool = Pool<False>;

pub mod conn;

/// A database connection that enforces writability and transaction state.
pub trait Connection {
    /// Indicates whether this is a writer connection or not. This can be used to constrain the
    /// allowed connections to only connections obtained from the writer pool, so that the user
    /// will be alerted to misuse.
    type Writable: Bool;
    /// Whether the connection is required to be engaged in an active transaction when the
    /// function is called. This can be used to enforce that a function is only called from
    /// within a transaction:
    ///
    /// ```
    /// use opsqueue::db::{Connection, magic::*, ReaderPool};
    /// use futures::FutureExt as _;
    ///
    /// async fn some_operation(
    ///   tx: impl Connection<Transaction = True>
    /// ) -> sqlx::Result<()> {
    ///     Ok(())
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> sqlx::Result<()> {
    /// # let pool = sqlx::SqlitePool::connect(":memory:").await.map(ReaderPool::new)?;
    /// let mut conn = pool.reader_conn().await?;
    /// conn.transaction(|tx| some_operation(tx).boxed()).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Calling the function like this would fail to compile, because `conn` is not in a transaction.
    ///
    /// ```compile_fail
    /// # use opsqueue::db::{TypedConnection, True, Pool};
    /// # async fn some_operation(tx: impl Connection<Transaction = True>) -> sqlx::Result<()> { Ok(()) }
    /// # #[tokio::main]
    /// # async fn main() -> sqlx::Result<()> {
    /// # let pool = sqlx::SqlitePool::connect(":memory:").await.map(ReaderPool::new)?;
    /// # let mut conn = pool.reader_conn().await?;
    /// some_operation(&mut conn).await?;
    /// # Ok(()) }
    /// ```
    type Transaction: Bool;

    /// Access the [`sqlx`] connection.
    fn get_inner(&mut self) -> &mut SqliteConnection;

    /// Execute a transaction.
    ///
    /// Within this transaction, you can call functions that require `Connection<Transaction = True>`.
    #[allow(async_fn_in_trait)]
    async fn transaction<O, E, F>(&mut self, f: F) -> Result<O, E>
    where
        for<'t> F: FnOnce(Conn<Self::Writable, Tx<'t, '_>>) -> BoxFuture<'t, Result<O, E>>
            + Send
            + Sync
            + 't,
        O: Send,
        E: From<sqlx::Error> + Send,
    {
        self.get_inner()
            .transaction(move |inner| {
                let conn = Conn::from_tx(inner);
                f(conn)
            })
            .await
    }
}

/// A writable database connection.
///
/// This is an alias for [`Connection`]. See its docs for details.
pub trait WriterConnection: Connection<Writable = True> {
    /// Whether this connection is currently in a transaction.
    ///
    /// See [`Connection::Transaction`] for details.
    type Transaction: Bool;
}

impl<T> WriterConnection for T
where
    T: Connection<Writable = True>,
{
    type Transaction = <T as Connection>::Transaction;
}

impl<C> Connection for &mut C
where
    C: Connection,
{
    type Writable = <C as Connection>::Writable;
    type Transaction = <C as Connection>::Transaction;

    fn get_inner(&mut self) -> &mut SqliteConnection {
        <C as Connection>::get_inner(*self)
    }
}

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
    read_pool: Pool<False>,
    write_pool: Pool<True>,
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
    /// We check whether we can not only reach the DB but especially if we can run a transaction.
    ///
    /// This handles the case where for whatever reason some other thing holds the write lock for
    /// the DB for a (too) long time.
    pub async fn check_health(&self) -> bool {
        match self.writer_conn().await {
            Ok(mut conn) => conn
                .transaction(|mut tx| {
                    Box::pin(async move {
                        let _count =
                            crate::common::submission::db::count_submissions(&mut tx).await?;
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
    /// Access the pool containing the reader connections.
    pub fn reader_pool(&self) -> &ReaderPool {
        &self.read_pool
    }
    /// Access the pool containing the writer connection.
    pub fn writer_pool(&self) -> &WriterPool {
        &self.write_pool
    }
    /// Access a reader connection.
    ///
    /// Such a connection can't be used to make changes to the state in the database.
    pub async fn reader_conn(&self) -> sqlx::Result<Reader<NoTransaction>> {
        self.read_pool.reader_conn().await
    }
    /// Access a writer connection.
    ///
    /// This connection can be used both for functions requiring read-only access and for functions
    /// that make changes to the state in the database.
    pub async fn writer_conn(&self) -> sqlx::Result<Writer<NoTransaction>> {
        self.write_pool.writer_conn().await
    }
}

/// A connection pool.
///
/// There are two flavors: [`WriterPool`] and [`ReaderPool`].
/// The former produces [`Writer`]s only, and the latter only [`Reader`]s. Reader connections can
/// only be used on functions where [`Connection::Writable`] is `False`.
#[derive(Debug, Clone)]
pub struct Pool<Writer> {
    pub(super) inner: SqlitePool,
    _type: PhantomData<Writer>,
}

impl<W> Pool<W>
where
    W: magic::Bool,
{
    /// Wrap the [`SqlitePool`] to add a type-level tag identifying it as either a reader or a writer pool.
    pub fn new(pool: SqlitePool) -> Pool<W> {
        Pool {
            inner: pool,
            _type: PhantomData,
        }
    }
    /// Acquire a new connection from the underlying pool and wrap it in our typed connection.
    pub async fn acquire(&self) -> sqlx::Result<Conn<W>> {
        self.inner.acquire().await.map(Conn::new)
    }
}

impl WriterPool {
    /// Acquire the connection capable of writing to the database.
    ///
    /// See [`DBPools`] for further explanation about readers and writers.
    ///
    /// [`DBPools`]: DBPools
    pub async fn writer_conn(&self) -> sqlx::Result<Writer<NoTransaction>> {
        self.acquire().await
    }
}

impl ReaderPool {
    /// Acquire one of the read-only database connections.
    ///
    /// See [`DBPools`] for further explanation about readers and writers.
    ///
    /// [`DBPools`]: DBPools
    pub async fn reader_conn(&self) -> sqlx::Result<Reader<NoTransaction>> {
        self.acquire().await
    }
}
pub mod magic {
    //! Home of the sealed [`Bool`] trait, used for indicating constraints on
    //! a [`Connection`][super::Connection].

    /// Type-level boolean that indicates that a property of a connection
    /// is true.
    #[derive(Debug, Clone, Copy)]
    pub enum True {}

    /// Type-level boolean that indicates that a property of a connection
    /// explicitly needs to be false.
    ///
    /// This could be used to disallow nesting transactions, for example:
    ///
    /// ```compile_fail
    /// use opsqueue::db::{Connection, True, Pool};
    /// use futures::FutureExt as _;
    ///
    /// // This function initiates a transaction, and let's pretend we
    /// // can't have nested transactions (for the sake of argument).
    /// async fn some_operation(
    ///   tx: impl Connection<Transaction = False>
    /// ) -> sqlx::Result<()> {
    ///     Ok(())
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> sqlx::Result<()> {
    /// # let pool = sqlx::SqlitePool::connect(":memory:").await.map(Pool::<False>::new)?;
    /// let mut conn = pool.reader_conn().await?;
    /// conn.run_tx(|tx| some_operation(tx).boxed()).await?;
    /// # Ok(()) }
    /// ```
    #[derive(Debug, Clone, Copy)]
    pub enum False {}

    mod s {
        pub trait Sealed {}
        impl Sealed for super::True {}
        impl Sealed for super::False {}
    }

    /// A type-level boolean used to express constraints on a connection.
    pub trait Bool: s::Sealed + Send + 'static {}

    impl Bool for True {}
    impl Bool for False {}
}

/// Connects to the SQLite database, creating it if it doesn't exist yet, and migrating it
/// if it isn't up to date.
///
/// This function should be called on app startup; it will panic when the database cannot be
/// opened or migrated.
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

fn db_options(database_filename: &str) -> SqliteConnectOptions {
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

async fn ensure_db_exists(database_filename: &str) {
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

async fn ensure_db_migrated(db: &WriterPool) {
    tracing::info!("Migrating backing DB");
    sqlx::migrate!("./migrations")
        // When rolling back, we want to be able to keep running even when the DB's schema is newer:
        .set_ignore_missing(true)
        .run(&db.inner)
        .await
        .expect("DB migrations failed");
    tracing::info!("Finished migrating backing DB");
}

/// Make the reader pool.
async fn db_connect_read_pool(
    database_filename: &str,
    max_read_pool_size: NonZero<u32>,
) -> ReaderPool {
    SqlitePoolOptions::new()
        .min_connections(16)
        .max_connections(max_read_pool_size.into())
        .connect_with(db_options(database_filename))
        .await
        .map(Pool::new)
        .expect("Could not connect to sqlite DB")
}

/// Connect the writer pool.
async fn db_connect_write_pool(database_filename: &str) -> WriterPool {
    SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .connect_with(db_options(database_filename))
        .await
        .map(Pool::new)
        .expect("Could not connect to sqlite DB")
}

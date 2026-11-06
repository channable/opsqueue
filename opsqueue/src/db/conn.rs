//! Concrete implementation of [`Connection`] and related types.

use super::{
    Connection,
    magic::{Bool, False, True},
};
use futures::future::BoxFuture;
use sqlx::{Connection as SqlxConnection, Error, Sqlite, SqliteConnection, pool::PoolConnection};
use std::marker::PhantomData;

/// A connection to the database.
///
/// This type is the concrete implementation of [`Connection`].
///
/// You can get one from a [`Pool`]; whether or not you can write with it is up to the type
/// of `Pool` you get it from. If you want a [`WriterConnection`], it needs to come from a
/// [`WriterPool`].
///
/// You can get a `WriterPool` from [`DBPools::writer_pool`], and a [`ReaderPool`] from
/// [`DBPools::reader_pool`].
///
/// [`Pool`]: super::Pool
/// [`ReaderPool`]: super::ReaderPool
/// [`WriterPool`]: super::WriterPool
/// [`DBPools::reader_pool`]: super::DBPools::reader_pool
/// [`DBPools::writer_pool`]: super::DBPools::writer_pool
/// [`WriterConnection`]: super::WriterConnection
pub struct Conn<Writable: Bool, Tx = NoTransaction> {
    inner: InnerConn<Tx>,
    _type: PhantomData<Writable>,
}

impl<W: Bool> Conn<W> {
    pub(super) fn new(inner: PoolConnection<Sqlite>) -> Conn<W> {
        Conn {
            inner: InnerConn::Pool(inner),
            _type: PhantomData,
        }
    }
    pub(super) fn from_tx<'tx, 'conn>(
        inner: &'tx mut sqlx::Transaction<'conn, Sqlite>,
        on_commit_callbacks: &'tx mut Vec<Box<dyn FnOnce() + Sync + Send>>,
    ) -> Conn<W, Tx<'tx, 'conn>> {
        Conn {
            inner: InnerConn::InTransaction(Tx {
                inner,
                on_commit_callbacks,
            }),
            _type: PhantomData,
        }
    }
}

/// A connection with read and write capabilities.
pub type Writer<Tx> = Conn<True, Tx>;

/// A connection without write capability.
pub type Reader<Tx> = Conn<False, Tx>;

/// A database transaction.
///
/// Used as a parameter to [`Conn`] when a transaction is active.
pub struct Tx<'tx, 'conn> {
    inner: &'tx mut sqlx::Transaction<'conn, sqlx::Sqlite>,
    on_commit_callbacks: &'tx mut Vec<Box<dyn FnOnce() + Sync + Send>>,
}

/// Used for the `Tx` slot of the [`Conn`] if we're not in a transaction.
///
/// This is an empty value that can never be populated.
pub enum NoTransaction {}

impl<Writable> Conn<Writable, NoTransaction>
where
    Writable: Bool,
{
    fn get_pool_connection(&mut self) -> &mut PoolConnection<Sqlite> {
        match &mut self.inner {
            InnerConn::Pool(pool_connection) => pool_connection,
            InnerConn::InTransaction(_) => {
                unreachable!("connection shouldn't have been in a transaction but it was")
            }
        }
    }
}

impl<Writable> Connection for Conn<Writable, NoTransaction>
where
    Writable: Bool,
{
    type Writable = Writable;
    type Transaction = False;
    fn get_inner(&mut self) -> &mut SqliteConnection {
        self.get_pool_connection()
    }

    async fn transaction<O, E, F>(&mut self, f: F) -> Result<O, E>
    where
        for<'t> F: FnOnce(Conn<Self::Writable, Tx<'t, '_>>) -> BoxFuture<'t, Result<O, E>>
            + Send
            + Sync
            + 't,
        O: Send,
        E: From<Error> + Send,
    {
        let mut on_commit_callbacks = Vec::new();
        // Note that we can't use `SqlxConnection::transaction` because it forces
        // `on_commit_callbacks` to have a `'static` lifetime. Therefore we use `begin`, `commit`
        // and `rollback` explicitly.
        let mut transaction = SqlxConnection::begin(self.get_inner()).await?;
        match f(Conn::from_tx(&mut transaction, &mut on_commit_callbacks)).await {
            Ok(result) => {
                transaction.commit().await?;

                for callback in on_commit_callbacks {
                    callback();
                }

                Ok(result)
            }
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    fn on_commit(&mut self, f: impl FnOnce()) {
        // Since we are not in a transaction, any mutations are already committed, so we run `f`
        // directly.
        f();
    }
}

impl<'tx, 'conn, Writable> Conn<Writable, Tx<'tx, 'conn>>
where
    Writable: Bool,
{
    fn get_tx<'a>(&'a mut self) -> &'a mut Tx<'tx, 'conn> {
        match &mut self.inner {
            InnerConn::Pool(_) => {
                unreachable!("connection should have been in a transaction but wasn't")
            }
            InnerConn::InTransaction(tx) => tx,
        }
    }
}

impl<Writable> Connection for Conn<Writable, Tx<'_, '_>>
where
    Writable: Bool,
{
    type Writable = Writable;
    type Transaction = True;

    fn get_inner(&mut self) -> &mut SqliteConnection {
        self.get_tx().inner
    }

    async fn transaction<O, E, F>(&mut self, f: F) -> Result<O, E>
    where
        for<'t> F: FnOnce(Conn<Self::Writable, Tx<'t, '_>>) -> BoxFuture<'t, Result<O, E>>
            + Send
            + Sync
            + 't,
        O: Send,
        E: From<Error> + Send,
    {
        let mut on_commit_callbacks = Vec::new();
        // Note that we can't use `SqlxConnection::transaction` because it forces
        // `on_commit_callbacks` to have a `'static` lifetime. Therefore we use `begin`, `commit`
        // and `rollback` explicitly.
        let mut transaction = SqlxConnection::begin(self.get_inner()).await?;
        match f(Conn::from_tx(&mut transaction, &mut on_commit_callbacks)).await {
            Ok(result) => {
                transaction.commit().await?;
                // Since this is a nested transaction we propagate the callbacks to the parent
                // transaction after the nested transaction commits.
                self.get_tx()
                    .on_commit_callbacks
                    .extend(on_commit_callbacks);
                Ok(result)
            }
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    fn on_commit(&mut self, f: impl FnOnce() + Send + Sync + 'static) {
        self.get_tx().on_commit_callbacks.push(Box::new(f));
    }
}

#[derive(Debug)]
enum InnerConn<Tx> {
    Pool(PoolConnection<Sqlite>),
    InTransaction(Tx),
}

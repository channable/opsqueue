//! Concrete implementation of [`Connection`] and related types.

use std::marker::PhantomData;

use sqlx::{pool::PoolConnection, Sqlite, SqliteConnection};

use super::{magic::*, Connection};

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
        inner: &'tx mut sqlx::Transaction<'conn, sqlx::Sqlite>,
    ) -> Conn<W, Tx<'tx, 'conn>> {
        Conn {
            inner: InnerConn::InTransaction(Tx(inner)),
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
pub struct Tx<'tx, 'conn>(&'tx mut sqlx::Transaction<'conn, sqlx::Sqlite>);

/// Used for the `Tx` slot of the [`Conn`] if we're not in a transaction.
///
/// This is an empty value that can never be populated.
pub enum NoTransaction {}

impl<Writable> Connection for Conn<Writable, NoTransaction>
where
    Writable: Bool,
{
    type Writable = Writable;
    type Transaction = False;
    fn get_inner(&mut self) -> &mut SqliteConnection {
        match &mut self.inner {
            InnerConn::Pool(pool_connection) => pool_connection,
            InnerConn::InTransaction(_) => unreachable!(),
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
        match &mut self.inner {
            InnerConn::Pool(pool_connection) => pool_connection,
            InnerConn::InTransaction(transaction) => transaction.0,
        }
    }
}

#[derive(Debug)]
enum InnerConn<Tx> {
    Pool(PoolConnection<Sqlite>),
    InTransaction(Tx),
}

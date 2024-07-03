// use crate::chunk::Chunk;
// use crate::submission::Submission;

// pub trait Persistence {
//     type DbResult<R>;
//     type TxConn<'t>;
//     fn atomically<'t, R, F: FnMut(&Self::TxConn<'t>) -> Self::DbResult<R>>(
//         &'t mut self,
//         fun: F,
//     ) -> Self::DbResult<R>;

//     fn insert_submission(&self, submission: &Submission) -> Self::DbResult<()>;

//     fn insert_chunk(&self, chunk: &Chunk) -> Self::DbResult<()>;

//     fn migrate(&self) -> Self::DbResult<()>;
// }

// impl Persistence for rusqlite::Connection {
//     type DbResult<R> = rusqlite::Result<R>;
//     type TxConn<'t> = rusqlite::Transaction<'t>;

//     fn atomically<'t, R, F: FnMut(&Self::TxConn<'t>) -> Self::DbResult<R>>(
//         &'t mut self,
//         mut fun: F,
//     ) -> Self::DbResult<R> {
//         let tx = self.transaction()?;
//         let res = fun(&tx);
//         let _ = tx.commit()?;
//         return res;
//     }

//     fn insert_submission(&self, submission: &Submission) -> Self::DbResult<()> {
//         let _ = self.execute("INSERT INTO submissions (id, chunks_total, chunks_done, metadata) VALUES (?1, ?2, ?3, ?4)", (&&submission.id, submission.chunks_total, &submission.chunks_done, &submission.metadata))?;
//         Ok(())
//     }

//     fn insert_chunk(&self, chunk: &Chunk) -> Self::DbResult<()> {
//         let _ = self.execute(
//             "INSERT INTO chunks (submission_id, id, uri) VALUES (?1,?2,?3)",
//             (&&chunk.submission_id, chunk.id, &chunk.uri),
//         )?;
//         Ok(())
//     }

//     fn migrate(&self) -> Self::DbResult<()> {
//         self.pragma_update(None, "journal_mode", "wal")?;
//         self.pragma_update(None, "synchronous", "normal")?;
//         self.pragma_update(None, "busy_timeout", "5000")?;

//         self.execute("CREATE TABLE IF NOT EXISTS submissions (id INTEGER PRIMARY KEY, chunks_total INTEGER, chunks_done INTEGER, metadata BLOB);", ())?;
//         self.execute("CREATE TABLE IF NOT EXISTS chunks (submission_id INTEGER, id INTEGER, uri BLOB, PRIMARY KEY (submission_id, id));", ())?;

//         Ok(())
//     }
// }

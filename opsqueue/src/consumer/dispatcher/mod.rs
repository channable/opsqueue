pub mod metastate;
pub mod reserver;

use crate::{
    common::{
        chunk::{Chunk, ChunkId, ChunkIndex},
        submission::{Submission, SubmissionId},
    },
    db::{Connection, Pool, ReaderPool, magic::Bool},
};
use futures::stream::{StreamExt as _, TryStreamExt as _};
use libsqlite3_sys as ffi;
use metastate::MetaState;
use reserver::Reserver;
use sqlx::QueryBuilder;
use std::ffi::CStr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

use std::sync::Arc;

use super::strategy;
use crate::common::StrategicMetadataMap;

pub unsafe extern "C" fn sqlite_reserved_chunk_lookup(
    context: *mut ffi::sqlite3_context,
    n_args: i32,
    args: *mut *mut ffi::sqlite3_value,
) {
    unsafe {
        if n_args != 2 {
            tracing::error!(
                n_args,
                "opsqueue_is_reserved called with unexpected argument count"
            );
            // Fail open: this callback is an optimization only.
            ffi::sqlite3_result_int(context, 0);
            return;
        }

        let user_data = ffi::sqlite3_user_data(context) as *const Reserver<ChunkId, ChunkId>;
        if user_data.is_null() {
            tracing::error!("opsqueue_is_reserved called without registered reserver user_data");
            // Fail open: this callback is an optimization only.
            ffi::sqlite3_result_int(context, 0);
            return;
        }

        let submission_id_raw = ffi::sqlite3_value_int64(*args.add(0));
        let chunk_index_raw = ffi::sqlite3_value_int64(*args.add(1));

        let Ok(submission_id) = SubmissionId::try_from(submission_id_raw) else {
            tracing::error!(
                submission_id_raw,
                "opsqueue_is_reserved got invalid submission_id"
            );
            // Fail open: this callback is an optimization only.
            ffi::sqlite3_result_int(context, 0);
            return;
        };
        let Ok(chunk_index) = ChunkIndex::try_from(chunk_index_raw) else {
            tracing::error!(
                chunk_index_raw,
                "opsqueue_is_reserved got invalid chunk_index"
            );
            // Fail open: this callback is an optimization only.
            ffi::sqlite3_result_int(context, 0);
            return;
        };

        let chunk_id = ChunkId::from((submission_id, chunk_index));
        let is_reserved = (*user_data).is_reserved(&chunk_id);
        ffi::sqlite3_result_int(context, i32::from(is_reserved));
    }
}

unsafe extern "C" fn sqlite_reserved_chunk_lookup_destructor(ptr: *mut std::ffi::c_void) {
    unsafe {
        if ptr.is_null() {
            return;
        }
        let _boxed: Box<Reserver<ChunkId, ChunkId>> = Box::from_raw(ptr.cast());
    }
}

unsafe extern "C" fn sqlite_metadata_count_lookup(
    context: *mut ffi::sqlite3_context,
    n_args: i32,
    args: *mut *mut ffi::sqlite3_value,
) {
    unsafe {
        if n_args != 2 {
            tracing::error!(
                n_args,
                "opsqueue_metadata_count called with unexpected argument count"
            );
            ffi::sqlite3_result_null(context);
            return;
        }

        let user_data = ffi::sqlite3_user_data(context) as *const Arc<MetaState>;
        if user_data.is_null() {
            tracing::error!(
                "opsqueue_metadata_count called without registered metastate user_data"
            );
            ffi::sqlite3_result_null(context);
            return;
        }

        let metadata_key_ptr = ffi::sqlite3_value_text(*args.add(0));
        if metadata_key_ptr.is_null() {
            ffi::sqlite3_result_null(context);
            return;
        }
        let Ok(metadata_key) = CStr::from_ptr(metadata_key_ptr.cast()).to_str() else {
            tracing::error!("opsqueue_metadata_count got non-utf8 metadata_key");
            ffi::sqlite3_result_null(context);
            return;
        };

        let metadata_value = ffi::sqlite3_value_int64(*args.add(1));

        if let Some(meta_count) = (*user_data)
            .get(metadata_key)
            .and_then(|meta_keys| meta_keys.get(&metadata_value))
        {
            ffi::sqlite3_result_int64(context, i64::try_from(meta_count).unwrap_or(i64::MAX));
        } else {
            ffi::sqlite3_result_null(context);
        }
    }
}

unsafe extern "C" fn sqlite_metadata_count_lookup_destructor(ptr: *mut std::ffi::c_void) {
    unsafe {
        if ptr.is_null() {
            return;
        }
        let _boxed: Box<Arc<MetaState>> = Box::from_raw(ptr.cast());
    }
}

#[derive(Debug, Clone)]
pub struct Dispatcher {
    reserver: Reserver<ChunkId, ChunkId>,
    metastate: Arc<MetaState>,
}

impl Dispatcher {
    #[must_use]
    pub fn new(reservation_expiration: Duration) -> Self {
        let reserver = Reserver::new(reservation_expiration);
        let metastate = Arc::new(MetaState::default());

        Dispatcher {
            reserver,
            metastate,
        }
    }

    #[must_use]
    pub fn metastate(&self) -> &MetaState {
        &self.metastate
    }

    #[must_use]
    pub fn reserver(&self) -> &Reserver<ChunkId, ChunkId> {
        &self.reserver
    }

    fn insert_metadata(&self, metadata: &StrategicMetadataMap) {
        for (name, value) in metadata {
            self.metastate.increment(name, *value);
        }
    }

    fn remove_metadata(&self, metadata: &StrategicMetadataMap) {
        for (name, value) in metadata {
            self.metastate.decrement(name, *value);
        }
    }

    /// Fetch chunks according to strategy and reserve them in memory.
    ///
    /// # Errors
    ///
    /// Returns an error if querying or joining submission info from the database fails.
    pub async fn fetch_and_reserve_chunks(
        &self,
        pool: &ReaderPool,
        strategy: strategy::Strategy,
        limit: usize,
        stale_chunks_notifier: &UnboundedSender<ChunkId>,
    ) -> Result<Vec<(Chunk, Submission)>, sqlx::Error> {
        let mut conn = pool.reader_conn().await?;
        self.register_lookups(conn.get_inner()).await?;
        let mut query_builder = QueryBuilder::new("");
        let stream = strategy
            .build_query(&mut query_builder)
            .build_query_as()
            .fetch(conn.get_inner());
        stream
            .try_filter_map(|chunk| self.reserve_chunk(chunk, stale_chunks_notifier))
            .and_then(|chunk| self.join_chunk_with_submission_info(chunk, pool))
            .take(limit)
            .try_collect()
            .await
    }

    pub async fn register_lookups(
        &self,
        conn: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx::Error> {
        let mut handle = conn.lock_handle().await?;
        let sqlite = handle.as_raw_handle().as_ptr();
        let reserved_function_name = b"opsqueue_is_reserved\0";
        let metadata_count_function_name = b"opsqueue_metadata_count\0";

        // Register the current reserver state on this connection.
        // Re-registering replaces any previous callback on this handle.
        let user_data = Box::new(self.reserver.clone());
        let user_data = Box::into_raw(user_data).cast::<std::ffi::c_void>();

        let rc = unsafe {
            ffi::sqlite3_create_function_v2(
                sqlite,
                reserved_function_name.as_ptr().cast(),
                2,
                ffi::SQLITE_UTF8,
                user_data,
                Some(sqlite_reserved_chunk_lookup),
                None,
                None,
                Some(sqlite_reserved_chunk_lookup_destructor),
            )
        };

        if rc != ffi::SQLITE_OK {
            unsafe { sqlite_reserved_chunk_lookup_destructor(user_data) };
            return Err(sqlx::Error::Protocol(format!(
                "sqlite3_create_function_v2 failed with rc={rc}"
            )));
        }

        // Register metadata count lookup backed by current metastate.
        // Re-registering replaces any previous callback on this handle.
        let user_data = Box::new(self.metastate.clone());
        let user_data = Box::into_raw(user_data).cast::<std::ffi::c_void>();

        let rc = unsafe {
            ffi::sqlite3_create_function_v2(
                sqlite,
                metadata_count_function_name.as_ptr().cast(),
                2,
                ffi::SQLITE_UTF8,
                user_data,
                Some(sqlite_metadata_count_lookup),
                None,
                None,
                Some(sqlite_metadata_count_lookup_destructor),
            )
        };

        if rc != ffi::SQLITE_OK {
            unsafe { sqlite_metadata_count_lookup_destructor(user_data) };
            return Err(sqlx::Error::Protocol(format!(
                "sqlite3_create_function_v2 failed with rc={rc}"
            )));
        }

        Ok(())
    }

    async fn reserve_chunk<E>(
        &self,
        chunk: Chunk,
        stale_chunks_notifier: &UnboundedSender<ChunkId>,
    ) -> Result<Option<Chunk>, E> {
        let chunk_id = ChunkId::from((chunk.submission_id, chunk.chunk_index));
        let val = self
            .reserver
            .try_reserve(chunk_id, chunk_id, stale_chunks_notifier)
            .map(|_| chunk);
        Ok(val)
    }

    async fn join_chunk_with_submission_info(
        &self,
        chunk: Chunk,
        pool: &Pool<impl Bool>,
    ) -> Result<(Chunk, Submission), sqlx::Error> {
        let mut conn = pool.acquire().await?;
        let submission =
            crate::common::submission::db::get_submission(chunk.submission_id, &mut conn)
                .await
                .expect("get_submission while reserving failed");
        let metadata = crate::common::submission::db::get_submission_strategic_metadata(
            chunk.submission_id,
            &mut conn,
        )
        .await
        .expect("get_submission_strategic_metadata while reserving failed");
        self.insert_metadata(&metadata);
        Ok((chunk, submission))
    }

    pub fn finish_reservations_sync<'a>(&self, reservations: impl Iterator<Item = &'a ChunkId>) {
        for reservation in reservations {
            let _ = self.reserver.finish_reservation_sync(reservation);
        }
    }

    pub async fn finish_reservation(
        &self,
        conn: impl Connection,
        id: ChunkId,
        delayed: bool,
    ) -> Option<Instant> {
        let maybe_started_at = self.reserver.finish_reservation(&id, delayed).await;

        // In the highly unlikely event that this DB query fails,
        // we still want to continue
        let metadata = crate::common::submission::db::get_submission_strategic_metadata(
            id.submission_id,
            conn,
        )
        .await
        .unwrap_or_default();

        self.remove_metadata(&metadata);
        maybe_started_at
    }

    pub fn run_pending_tasks_periodically(&self, cancellation_token: CancellationToken) {
        self.reserver
            .run_pending_tasks_periodically(cancellation_token);
    }
}

#[cfg(test)]
#[cfg(feature = "server-logic")]
mod test {
    use super::*;
    use crate::common::chunk::ChunkId;
    use crate::common::chunk::ChunkSize;
    use crate::db::DBPools;
    use tokio::sync::mpsc::unbounded_channel;
    use ux::u63;

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn fetch_and_reserve_chunks_excludes_already_reserved(db: sqlx::SqlitePool) {
        let pools = DBPools::from_test_pool(&db);
        let dispatcher = Dispatcher::new(Duration::from_mins(1));
        let (stale_chunks_notifier, mut _stale_chunks_receiver) = unbounded_channel::<ChunkId>();

        let mut writer_conn = pools.writer_conn().await.unwrap();
        let submission_id = crate::common::submission::db::insert_submission_from_chunks(
            None,
            vec![Some("a".into()), Some("b".into()), Some("c".into())],
            None,
            StrategicMetadataMap::default(),
            ChunkSize::default(),
            &mut writer_conn,
        )
        .await
        .unwrap();

        let pre_reserved_chunk = ChunkId::from((submission_id, u63::new(0).into()));
        dispatcher
            .reserver()
            .try_reserve(
                pre_reserved_chunk,
                pre_reserved_chunk,
                &stale_chunks_notifier,
            )
            .expect("precondition: pre-reserving chunk should succeed");

        let reserved = dispatcher
            .fetch_and_reserve_chunks(
                pools.reader_pool(),
                strategy::Strategy::Oldest,
                10,
                &stale_chunks_notifier,
            )
            .await
            .unwrap();

        assert_eq!(reserved.len(), 2);
        assert!(reserved.iter().all(|(chunk, _submission)| {
            ChunkId::from((chunk.submission_id, chunk.chunk_index)) != pre_reserved_chunk
        }));
    }
}

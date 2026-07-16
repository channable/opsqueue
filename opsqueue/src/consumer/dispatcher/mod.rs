pub mod metastate;
pub mod reserver;

use crate::{
    common::{
        chunk::{Chunk, ChunkId, ChunkIndex},
        submission::{Submission, SubmissionId},
    },
    db::{magic::Bool, Connection, Pool, ReaderPool},
};
use futures::stream::{StreamExt as _, TryStreamExt as _};
use libsqlite3_sys as ffi;
use metastate::MetaState;
use reserver::Reserver;
use sqlx::QueryBuilder;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

use std::sync::Arc;

use super::strategy;
use crate::common::StrategicMetadataMap;

unsafe extern "C" fn sqlite_reserved_chunk_lookup(
    context: *mut ffi::sqlite3_context,
    n_args: i32,
    args: *mut *mut ffi::sqlite3_value,
) {
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

unsafe extern "C" fn sqlite_reserved_chunk_lookup_destructor(ptr: *mut std::ffi::c_void) {
    if ptr.is_null() {
        return;
    }
    let _boxed: Box<Reserver<ChunkId, ChunkId>> = Box::from_raw(ptr.cast());
}

#[derive(Debug, Clone)]
pub struct Dispatcher {
    reserver: Reserver<ChunkId, ChunkId>,
    metastate: Arc<MetaState>,
}

impl Dispatcher {
    pub fn new(reservation_expiration: Duration) -> Self {
        let reserver = Reserver::new(reservation_expiration);
        let metastate = Arc::new(MetaState::default());

        Dispatcher {
            reserver,
            metastate,
        }
    }

    pub fn metastate(&self) -> &MetaState {
        &self.metastate
    }

    pub fn reserver(&self) -> &Reserver<ChunkId, ChunkId> {
        &self.reserver
    }

    fn insert_metadata(&self, metadata: &StrategicMetadataMap) {
        for (name, value) in metadata {
            self.metastate.increment(name, value);
        }
    }

    fn remove_metadata(&self, metadata: &StrategicMetadataMap) {
        for (name, value) in metadata {
            self.metastate.decrement(name, value);
        }
    }

    pub async fn fetch_and_reserve_chunks(
        &self,
        pool: &ReaderPool,
        strategy: strategy::Strategy,
        limit: usize,
        stale_chunks_notifier: &UnboundedSender<ChunkId>,
    ) -> Result<Vec<(Chunk, Submission)>, sqlx::Error> {
        let mut conn = pool.reader_conn().await?;
        self.register_reserved_chunk_lookup(conn.get_inner())
            .await?;
        let mut query_builder = QueryBuilder::new("");
        let stream = strategy
            .build_query(&mut query_builder, &self.metastate)
            .build_query_as()
            .fetch(conn.get_inner());
        stream
            .try_filter_map(|chunk| self.reserve_chunk(chunk, stale_chunks_notifier))
            .and_then(|chunk| self.join_chunk_with_submission_info(chunk, pool))
            .take(limit)
            .try_collect()
            .await
    }

    async fn register_reserved_chunk_lookup(
        &self,
        conn: &mut sqlx::SqliteConnection,
    ) -> Result<(), sqlx::Error> {
        let mut handle = conn.lock_handle().await?;
        let sqlite = handle.as_raw_handle().as_ptr();
        let function_name = b"opsqueue_is_reserved\0";

        // Register the current reserver state on this connection.
        // Re-registering replaces any previous callback on this handle.
        let user_data = Box::new(self.reserver.clone());
        let user_data = Box::into_raw(user_data).cast::<std::ffi::c_void>();

        let rc = unsafe {
            ffi::sqlite3_create_function_v2(
                sqlite,
                function_name.as_ptr().cast(),
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

    #[sqlx::test]
    async fn fetch_and_reserve_chunks_excludes_already_reserved(db: sqlx::SqlitePool) {
        let pools = DBPools::from_test_pool(&db);
        let dispatcher = Dispatcher::new(Duration::from_secs(60));
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

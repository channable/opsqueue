use std::{
    fmt::Debug,
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};

use axum_prometheus::metrics::{counter, gauge};
use moka::{notification::RemovalCause, sync::Cache};
use rustc_hash::FxBuildHasher;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::time::DelayQueue;

#[derive(Clone)]
pub struct Reserver<K, V> {
    reservations: Cache<K, (V, UnboundedSender<V>, Instant), FxBuildHasher>,
    pending_removals: Arc<Mutex<DelayQueue<K>>>,
}

impl<K, V> core::fmt::Debug for Reserver<K, V>
where
    K: Hash + Eq + Send + Sync + Debug + Copy + 'static,
    V: Send + Sync + Clone + Debug + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reserver")
            .field("reservations", &self.reservations)
            .field("&pending_removals", &self.pending_removals)
            .finish()
    }
}

impl<K, V> Reserver<K, V>
where
    K: Hash + Eq + Send + Sync + Debug + Copy + 'static,
    V: Send + Sync + Clone + 'static,
{
    pub fn new(reservation_expiration: Duration) -> Self {
        let reservations = Cache::builder()
            .time_to_live(reservation_expiration)
            .eviction_listener(|_key, val: (V, UnboundedSender<V>, _), cause| {
                if cause == RemovalCause::Expired {
                    // Only error case is if receiver is no longer listening
                    // In that case, nobody cares about the value being evicted anymore.
                    // So `let _ =` is correct here.
                    let _ = val.1.send(val.0);
                }
            })
            // We're not worried about HashDoS as the consumers are trusted code,
            // so let's use a faster hash than SipHash
            .build_with_hasher(rustc_hash::FxBuildHasher);
        let pending_removals = Default::default();
        Reserver {
            reservations,
            pending_removals,
        }
    }

    // pub fn metastate(&self) -> &MetaState {
    //     &self.metastate
    // }

    /// Attempts to reserve a particular key-val.
    ///
    /// Returns `None` if someone else currently is already reserving `key`.
    #[must_use]
    #[tracing::instrument(level = "debug", skip(self, val, sender))]
    pub fn try_reserve(&self, key: K, val: V, sender: &UnboundedSender<V>) -> Option<V> {
        let entry = self
            .reservations
            .entry(key)
            .or_insert_with(|| (val, sender.clone(), Instant::now()));

        if entry.is_fresh() {
            tracing::debug!("Reservation of {key:?} succeeded!");
            counter!(crate::prometheus::RESERVER_RESERVATIONS_SUCCEEDED_COUNTER).increment(1);

            Some(entry.into_value().0)
        } else {
            // Someone else reserved this first
            tracing::trace!("Reservation of {key:?} failed!");
            counter!(crate::prometheus::RESERVER_RESERVATIONS_FAILED_COUNTER).increment(1);
            None
        }
    }

    /// Removes a particular key-val from the reserver.
    /// Afterwards, it is possible to reserve it again.
    ///
    /// Precondition: key should be reserved first (checked in debug builds)
    pub async fn finish_reservation(&self, key: &K, delayed: bool) -> Option<Instant> {
        match self.reservations.get(key) {
            None => {
                tracing::warn!("Attempted to finish non-existent reservation: {key:?}");
                None
            }
            Some((_val, _sender, reserved_at)) => {
                if delayed {
                    // We remove the reservation with a slight delay
                    // This is to prevent a race condition where a SQLite read cursor
                    // (such as those from `ConsumerState::fetch_and_reserve_chunks`) still uses an older, stale version of the data.
                    // This could cause
                    // re-reserving a chunk after it was completed
                    // c.f. https://github.com/channable/opsqueue/issues/96
                    self.pending_removals
                        .lock()
                        .await
                        .insert(*key, Duration::from_secs(1));
                    Some(reserved_at)
                } else {
                    self.reservations.remove(key);
                    Some(reserved_at)
                }
            }
        }
    }

    /// Sync version of `.finish_reservation`; does not support delayed removal.
    /// Intended to be called from within a `Drop` implementation.
    pub fn finish_reservation_sync(&self, key: &K) -> Option<Instant> {
        self.reservations
            .remove(key)
            .map(|(_val, _sender, reserved_at)| reserved_at)
    }

    /// Run this every so often to make sure outdated entries are cleaned up
    /// (have their cleanup handlers called and their memory freed)
    ///
    /// In production code, use `run_pending_tasks_periodically` instead.
    /// In tests, we call this when we want to make the tests deterministic.
    pub async fn run_pending_tasks(&self) {
        self.purge_pending_removals().await;
        self.reservations.run_pending_tasks();
        // By running this immediately after 'run_pending_tasks',
        // we can be reasonably sure that the count is accurate (doesn't include expired entries),
        // c.f. documentation of moka::sync::Cache::entry_count.
        gauge!(crate::prometheus::RESERVER_CHUNKS_RESERVED_GAUGE)
            .set(self.reservations.entry_count() as u32);
    }

    /// Purges all reservations that were finished with `delayed: true` earlier,
    /// whose delay has since passed
    async fn purge_pending_removals(&self) {
        use futures::StreamExt;
        let mut removals = self.pending_removals.lock().await;
        loop {
            tokio::select! {
                biased;
                Some(entry) = removals.next() => {
                    tracing::trace!("Removing outdated reservation: {entry:?}");
                    self.reservations.remove(entry.get_ref());
                }
                _ = async {} => break,
            }
        }
    }

    /// Call this _once_ to have the reserver set up a background task
    /// that will call `run_pending_tasks` periodically.
    ///
    /// Do not call this in tests.
    pub fn run_pending_tasks_periodically(&self, cancellation_token: CancellationToken) {
        let bg_reserver_handle = self.clone();
        tokio::spawn(async move {
            loop {
                bg_reserver_handle.run_pending_tasks().await;
                tokio::select! {
                    () = cancellation_token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            }
        });
    }
}

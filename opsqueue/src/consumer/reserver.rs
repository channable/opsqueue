use std::{fmt::Debug, hash::Hash, time::{Duration, Instant}};

use axum_prometheus::metrics::{counter, gauge};
use moka::{notification::RemovalCause, sync::Cache};
use rustc_hash::FxBuildHasher;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct Reserver<K, V>(Cache<K, (V, UnboundedSender<V>, Instant), FxBuildHasher>);

impl<K, V> core::fmt::Debug for Reserver<K, V>
where
    K: Hash + Eq + Send + Sync + Debug + Copy + 'static,
    V: Send + Sync + Clone + Debug + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Reserver").field(&self.0).finish()
    }
}

impl<K, V> Reserver<K, V>
where
    K: Hash + Eq + Send + Sync + Debug + Copy + 'static,
    V: Send + Sync + Clone + 'static,
{
    pub fn new(reservation_expiration: Duration) -> Self {
        let cache = Cache::builder()
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
        Reserver(cache)
    }

    /// Attempts to reserve a particular key-val.
    ///
    /// Returns `None` if someone else currently is already reserving `key`.
    #[must_use]
    #[tracing::instrument(level="debug", skip(self, val, sender))]
    pub fn try_reserve(&self, key: K, val: V, sender: &UnboundedSender<V>) -> Option<V> {
        let entry = self.0.entry(key).or_insert_with(|| (val, sender.clone(), Instant::now()));

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
    pub fn finish_reservation(&self, key: &K) -> Option<Instant> {
        match self.0.remove(key) {
            None => {
                tracing::warn!("Attempted to finish non-existent reservation: {key:?}");
                None
            },
            Some((_val, _sender, reserved_at)) => {
                Some(reserved_at)
            }

        }
    }

    /// Run this every so often to make sure outdated entries are cleaned up
    /// (have their cleanup handlers called and their memory freed)
    ///
    /// In production code, use `run_pending_tasks_periodically` instead.
    /// In tests, we call this when we want to make the tests deterministic.
    pub fn run_pending_tasks(&self) {
        self.0.run_pending_tasks()
    }

    /// Call this _once_ to have the reserver set up a background task
    /// that will call `run_pending_tasks` periodically.
    ///
    /// Do not call this in tests.
    pub fn run_pending_tasks_periodically(
        &self,
        cancellation_token: CancellationToken,
    ) {
        let bg_reserver_handle = self.clone();
        tokio::spawn(async move {
            loop {
                bg_reserver_handle.run_pending_tasks();
                // By running this immediately after 'run_pending_tasks',
                // we can be reasonably sure that the count is accurate (doesn't include expired entries),
                // c.f. documentation of moka::sync::Cache::entry_count.
                gauge!(crate::prometheus::RESERVER_CHUNKS_RESERVED_GAUGE).set(bg_reserver_handle.0.entry_count() as u32);
                tokio::select! {
                    () = cancellation_token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        });
    }
}

use std::{fmt::Debug, hash::Hash, time::Duration};

use moka::{notification::RemovalCause, sync::Cache};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Clone)]
pub struct Reserver<K, V>(Cache<K, (V, UnboundedSender<V>)>);

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
            .eviction_listener(|_key, val: (V, UnboundedSender<V>), cause| {
                if cause == RemovalCause::Expired {
                    // Only error case is if receiver is no longer listening
                    // In that case, nobody cares about the value being evicted anymore.
                    // So `let _ =` is correct here.
                    let _ = val.1.send(val.0);
                }
            })
            .build();
        Reserver(cache)
    }

    /// Attempts to reserve a particular key-val.
    ///
    /// Returns `None` if someone else currently is already reserving `key`.
    #[must_use]
    pub fn try_reserve(&self, key: K, val: V, sender: &UnboundedSender<V>) -> Option<V> {
        let entry = self.0.entry(key).or_insert_with(|| (val, sender.clone()));
        let res = if entry.is_fresh() {
            tracing::debug!("Reservation of {key:?} succeeded!");
            // Reservation succeeded
            Some(entry.into_value().0)
        } else {
            tracing::debug!("Reservation of {key:?} failed!");
            // Someone else reserved this first
            None
        };

        assert!(self.0.contains_key(&key));

        res
    }

    /// Removes a particular key-val from the reserver.
    /// Afterwards, it is possible to reserve it again.
    ///
    /// Precondition: key should be reserved first (checked in debug builds)
    pub fn finish_reservation(&self, key: &K) {
        self.0.invalidate(key)
        // let res = self.0.remove(key);
        // if !res.is_some() {
        //     tracing::error!("Attempted to finish non-existent reservation: {key:?}");
        // }
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
    pub fn run_pending_tasks_periodically(&self, cancellation_token: CancellationToken, task_tracker: TaskTracker) {
        let bg_reserver_handle = self.clone();
        task_tracker.spawn(async move {
            loop {
                bg_reserver_handle.run_pending_tasks();
                tokio::select! {
                    () = cancellation_token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        });
    }
}

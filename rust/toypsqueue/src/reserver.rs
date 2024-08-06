use std::{fmt::Debug, hash::Hash, time::Duration};

use moka::{notification::RemovalCause, sync::Cache};

/// An in-memory datastructure that ensures that only one client can reserve a particular key at a time.
/// 
/// Internally this is implemented using an (unbounded-size) cache, but the cache is used in the 'opposite' way:
/// Only values which are _not_ in the cache yet are returned (and then added to the cache).
/// 
/// This is essentially implements the FOR UPDATE SKIP LOCKED style row selection
/// that some other databases offer, except it is more flexible.
/// 
/// Since this is an in-memory-only structure,
/// once the program restarts after quitting, any and all reservations are trivially cleared.
#[derive(Clone)]
#[repr(transparent)]
pub struct Reserver<K, V, CleanupFun>(Cache<K, (V, CleanupFun)>);

impl<K, V, CleanupFun> core::fmt::Debug for Reserver<K, V, CleanupFun>
where
    Cache<K, (V, CleanupFun)>: core::fmt::Debug,
    CleanupFun: FnOnce(V),
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Reserver").field(&self.0).finish()
    }
}

impl<K, V, CleanupFun> Reserver<K, V, CleanupFun>
where
    K: Eq + Hash + Send + Sync + 'static,
    (V, CleanupFun): Clone + Send + Sync + 'static,
    V: Debug,
    CleanupFun: FnOnce(V),
{
    pub fn new() -> Self {
        let cache = Cache::builder()
            .time_to_live(Duration::from_secs(1))
            .eviction_listener(|_key, val: (V, CleanupFun), cause| {
                if cause == RemovalCause::Expired {
                    val.1(val.0)
                };
            })
            .build();
        Reserver(cache)
    }

    /// Attempts to reserve a particular key-val.
    /// 
    /// Returns `None` if someone else currently is already reserving `key`.
    #[must_use]
    pub fn try_reserve(&self, key: K, val: V, cleanup: CleanupFun) -> Option<V> {
        let entry = self.0.entry(key).or_insert_with(|| (val, cleanup));
        if entry.is_fresh() {
            // Reservation succeeded
            Some(entry.into_value().0)
        } else {
            // Someone else reserved this first
            None
        }
    }

    /// Removes a particular key-val from the reserver.
    /// Afterwards, it is possible to reserve it again.
    /// 
    /// Precondition: key should be reserved first (checked in debug builds)
    pub fn finish_reservation(&self, key: &K) {
        let res = self.0.remove(&key);
        debug_assert!(res.is_some(), "Attempted to finish non-existent reservation");
    }

    /// Run this every so often to make sure outdated entries are cleaned up
    /// (have their cleanup handlers called and their memory freed)
    pub fn run_pending_tasks(&self) {
        self.0.run_pending_tasks()
    }
}

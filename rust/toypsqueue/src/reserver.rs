use std::{fmt::Debug, hash::Hash, time::Duration};

use moka::{notification::RemovalCause, sync::Cache};

#[derive(Clone)]
#[repr(transparent)]
pub struct Reserver<K, V, F>(Cache<K, (V, F)>);

impl<K, V, F> core::fmt::Debug for Reserver<K, V, F>
where
    Cache<K, (V, F)>: core::fmt::Debug,
    F: FnOnce(V),
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Reserver").field(&self.0).finish()
    }
}

impl<K, V, F> Reserver<K, V, F>
where
    K: Eq + Hash + Send + Sync + 'static,
    (V, F): Clone + Send + Sync + 'static,
    V: Debug,
    F: FnOnce(V),
{
    pub fn new() -> Self {
        let cache = Cache::builder()
            .time_to_live(Duration::from_secs(1))
            .eviction_listener(|_key, val: (V, F), cause| {
                if cause == RemovalCause::Expired {
                    val.1(val.0)
                };
            })
            .build();
        Reserver(cache)
    }

    #[must_use]
    pub fn try_reserve(&self, key: K, val: V, f: F) -> Option<V> {
        let entry = self.0.entry(key).or_insert_with(|| (val, f));
        if entry.is_fresh() {
            // Reservation succeeded
            Some(entry.into_value().0)
        } else {
            // Someone else reserved this first
            None
        }
    }

    pub fn run_pending_tasks(&self) {
        self.0.run_pending_tasks()
    }
}

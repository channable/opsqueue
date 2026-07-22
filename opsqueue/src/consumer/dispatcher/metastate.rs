use dashmap::{DashMap, Entry};
use rustc_hash::FxBuildHasher;
use tracing;

#[derive(Debug, Default)]
pub struct MetaState(DashMap<String, MetaStateField>);

impl MetaState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment(&self, key: &str, val: MetaStateVal) {
        match self.0.get(key) {
            Some(meta_state_field) => meta_state_field.increment(val),
            None => self.0.entry(key.to_string()).or_default().increment(val),
        }
    }

    pub fn decrement(&self, key: &str, val: MetaStateVal) {
        let ripe_for_removal = {
            if let Some(meta_state_field) = self.0.get(key) {
                meta_state_field.decrement(val);
                meta_state_field.is_empty()
            } else {
                // The decrement is called unconditionally after `finish_reservation` happened. An
                // expired reservation always sends a chunk failure via the client, regardless of
                // whether `finish_reservation` succeeded or not. In other words, the `decrement` is
                // not always part of the same atomic state transition as removing the reservation
                // from the reserver. This means that if a reservation is finished by multiple
                // different racing paths we can get multiple decrements.
                tracing::error!(
                    key = key,
                    val = val,
                    "decrements should be paired with increments."
                );
                false
            }
        };
        if ripe_for_removal {
            // The actual removal happens after the main code to ensure we don't take out two locks
            // on the DashMap at the same time. To handle with the case of a concurrent increment,
            // we use `remove_if` and repeat the `is_empty` check.
            self.0.remove_if(key, |_k, v| v.is_empty());
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<dashmap::mapref::one::Ref<'_, String, MetaStateField>> {
        self.0.get(key)
    }
}

pub type Bytes = Vec<u8>;

/// As values, we support the largest number value `SQLite` supports by itself,
/// which should be sufficient for most 'ID' fields, which is what this feature is intended for.
///
/// If you really need to use strings or UUIDs with a `PreferDistinct` strategy,
/// consider hashing them and using that hash as `MetaStateVal`.
pub type MetaStateVal = i64;

#[derive(Debug, Default)]
pub struct MetaStateField {
    vals_to_counts: DashMap<MetaStateVal, usize, FxBuildHasher>,
}

impl MetaStateField {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn increment(&self, val: MetaStateVal) {
        self.vals_to_counts
            .entry(val)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    fn decrement(&self, val: MetaStateVal) {
        if let Entry::Occupied(entry) = self
            .vals_to_counts
            .entry(val)
            .and_modify(|count| *count -= 1)
            && *entry.get() == 0
        {
            entry.remove();
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.vals_to_counts.is_empty()
    }

    #[must_use]
    pub fn get(&self, val: &MetaStateVal) -> Option<usize> {
        self.vals_to_counts.get(val).map(|count| *count)
    }
}

#[cfg(test)]
mod tests {
    use tokio::task::JoinSet;

    use super::*;

    #[test]
    pub fn pairwise_incdec_results_in_empty_map() {
        use rand::seq::SliceRandom;
        let n_operations = 10_000;
        let n_groups = 100;
        let group_size = n_operations / n_groups;
        let sut = MetaState::new();

        let key = "company_id";
        let mut vals: Vec<_> = (0..n_operations)
            .map(|x| x % n_groups)
            .map(|val| i64::try_from(val).expect("test value fits into i64"))
            .collect();

        // Increment in one order
        vals.shuffle(&mut rand::rng());
        for &val in &vals {
            sut.increment(key, val);
        }

        {
            // We have to release the selected state_field before we can decrement it, otherwise we
            // would deadlock on the DashMap lock.
            let state_field = sut.get(key).expect("Should exist at this stage");
            for group in 0..n_groups {
                assert_eq!(state_field.get(&(group as i64)), Some(group_size));
            }
        }

        // Decrement in a different order
        vals.shuffle(&mut rand::rng());
        for &val in &vals {
            sut.decrement(key, val);
        }

        assert!(sut.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    pub async fn multithreaded_pairwise_incdec_results_in_empty_map() {
        use std::sync::Arc;
        let count = 10_000;
        let groups = 100;
        let vals: Vec<_> = (0..count).map(|x| x % groups).collect();

        let sut = Arc::new(MetaState::new());

        let key = "user_id";
        let mut task_set = JoinSet::new();
        for val in vals {
            let sut = sut.clone();
            task_set.spawn(async move {
                sut.increment(key, val);
                tokio::task::yield_now().await;
                sut.decrement(key, val);
            });
        }
        task_set.join_all().await;

        assert!(sut.is_empty());
    }
}

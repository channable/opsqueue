use crossbeam_skiplist::SkipSet;
use dashmap::{DashMap, Entry};
use rustc_hash::{FxBuildHasher};

#[derive(Debug, Default)]
pub struct MetaState(DashMap<String, MetaStateField>);

impl MetaState {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn increment(&self, key: &str, val: &[u8]) {
        match self.0.get(key) {
            Some(meta_state_field) => meta_state_field.increment(val),
            None => self.0.entry(key.to_string()).or_default().increment(val),
        }
    }
    pub fn decrement(&self, key: &str, val: &[u8]) {
        let ripe_for_removal = {
            let meta_state_field = self.0.get(key).expect("decrements should be paired with increments.");
            meta_state_field.decrement(val);
            meta_state_field.is_empty()
        };
        if ripe_for_removal {
            // The actual removal happens after the main code 
            // to ensure we don't take out two locks on the DashMap at the same time.
            // To handle with the case of a concurrent increment, we use `remove_if` and repeat the `is_empty` check.
            self.0.remove_if(key, |_k, v| v.is_empty());
        }
    }

    pub fn is_empty(&self) -> bool{
        self.0.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<dashmap::mapref::one::Ref<'_, String, MetaStateField>> {
        self.0.get(key)
    }
}

use std::sync::Arc;
pub type Bytes = Vec<u8>;
// optimization possibility: Reduce cloning by storing a leaked Box<str> in the DashMap,
// allowing us to use string references everywhere.
// Unfortunately this (i.e. `Box::from_raw` on removal of a key or on drop) requires a sprinkle of unsafe.
#[derive(Debug, Default)]
pub struct MetaStateField {
    vals_to_counts: DashMap<Arc<Bytes>, usize, FxBuildHasher>,
    counts_to_vals: SkipSet<(usize, Arc<Bytes>)>,
}

impl MetaStateField {
    pub fn new() -> Self {
        Default::default()
    }

    fn increment(&self, val: &[u8]) {
        match self.vals_to_counts.entry(Arc::new(val.to_vec())) {
            Entry::Vacant(entry) => {
                self.counts_to_vals.insert((1, entry.key().clone()));
                entry.insert(1);
            },
            Entry::Occupied(mut entry) => {
                // The entry is now locked, so we can also safely update the relevant element of the SkipSet
                let count = entry.get();
                let mut set_entry = (*count, entry.key().clone());
                self.counts_to_vals.remove(&set_entry);
                set_entry.0 += 1;
                self.counts_to_vals.insert(set_entry);
                *entry.get_mut() += 1;
            },
        }
    }

    fn decrement(&self, val: &[u8]) {
        match self.vals_to_counts.entry(Arc::new(val.to_vec())) {
            Entry::Vacant(_entry) => {
                unreachable!()
            },
            Entry::Occupied(mut entry) => {
                // The entry is now locked, so we can also safely update the relevant element of the SkipSet
                let count = entry.get();
                let mut set_entry = (*count, entry.key().clone());
                if *count == 1 {
                    *entry.get_mut() -= 1;
                    self.counts_to_vals.remove(&set_entry);
                    Some(set_entry.1);
                    entry.remove();
                } else {
                    *entry.get_mut() -= 1;
                    self.counts_to_vals.remove(&set_entry);
                    set_entry.0 -= 1;
                    self.counts_to_vals.insert(set_entry);
                }
            },
        };
    }

    pub fn is_empty(&self) -> bool {
        self.vals_to_counts.is_empty()
    }

    pub fn too_high_counts(& self, max: usize) -> impl Iterator<Item = Arc<Bytes>> + '_ {
        self.counts_to_vals.range((max, Arc::new(vec![]))..).map(|entry| entry.value().1.clone())
    }
}


#[cfg(test)]
mod tests {
    use tokio::task::JoinSet;

    use super::*;

    #[test]
    pub fn pairwise_incdec_results_in_empty_map() {
        use rand::seq::SliceRandom;
        let n_operations: usize = 10_000;
        let n_groups : usize = 100;
        let group_size = n_operations / n_groups;
        let sut = MetaState::new();

        let key = "company_id";
        let mut vals: Vec<_> = (0..n_operations).map(|x| x % n_groups).map(|val| val.to_string().into_bytes()).collect();

        // Increment in one order
        vals.shuffle(&mut rand::thread_rng());
        for val in &vals {
            sut.increment(key, val);
        }

        dbg!(&sut);

        let too_highs: Vec<_> = sut.get(key).expect("Should exist at this stage").too_high_counts(group_size).collect();
        assert_eq!(too_highs.len(), n_groups);

        // Decrement in a different order
        vals.shuffle(&mut rand::thread_rng());
        for val in &vals {
            sut.decrement(key, val);
        }

        assert!(sut.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    pub async fn multithreaded_pairwise_incdec_results_in_empty_map() {
        use std::sync::Arc;
        let count = 10_000;
        let groups = 100;
        let vals: Vec<_> = (0..count).map(|x| x % groups).map(|val| val.to_string().into_bytes()).collect();

        let sut = Arc::new(MetaState::new());

        let key = "user_id";
        let mut task_set = JoinSet::new();
        for val in vals {
            let sut = sut.clone();
            task_set.spawn(async move {
                sut.increment(key, &val);
                tokio::task::yield_now().await;
                sut.decrement(key, &val);
            });
        }
        task_set.join_all().await;

        assert!(sut.is_empty());
    }
}

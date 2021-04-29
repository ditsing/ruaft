use std::marker::PhantomData;
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};
use serde::Serialize;

use ruaft::Snapshot;
use serde::de::DeserializeOwned;

#[derive(Default)]
pub(crate) struct SnapshotHolder<T> {
    snapshot_requests: Mutex<Vec<(usize, Arc<Condvar>)>>,
    current_snapshot: Mutex<Snapshot>,
    phantom: PhantomData<T>,
}

impl<T> SnapshotHolder<T> {
    pub fn request_snapshot(&self, min_index: usize) -> Snapshot {
        let condvar = {
            let mut requests = self.snapshot_requests.lock();
            let pos =
                requests.binary_search_by_key(&min_index, |&(index, _)| index);
            match pos {
                Ok(pos) => requests[pos].1.clone(),
                Err(pos) => {
                    assert!(pos == 0 || requests[pos - 1].0 < min_index);
                    assert!(
                        pos == requests.len() - 1
                            || requests[pos + 1].0 > min_index
                    );
                    let condvar = Arc::new(Condvar::new());
                    requests.insert(pos, (min_index, condvar.clone()));
                    condvar
                }
            }
        };

        // Now wait for the snapshot
        let mut current_snapshot = self.current_snapshot.lock();
        while current_snapshot.last_included_index < min_index {
            condvar.wait(&mut current_snapshot);
        }

        current_snapshot.clone()
    }
}

impl<T: Serialize> SnapshotHolder<T> {
    const MIN_SNAPSHOT_INDEX_INTERVAL: usize = 100;
    pub fn take_snapshot(&self, state: &T, curr: usize) {
        let expired = self.current_snapshot.lock().last_included_index
            + Self::MIN_SNAPSHOT_INDEX_INTERVAL
            <= curr;
        let requested = self
            .snapshot_requests
            .lock()
            .first()
            .map_or(false, |&(min_index, _)| min_index <= curr);

        if expired || requested {
            let data = bincode::serialize(state)
                .expect("Serialization should never fail.");
            let mut current_snapshot = self.current_snapshot.lock();
            assert!(current_snapshot.last_included_index < curr);
            *current_snapshot = Snapshot {
                last_included_index: curr,
                data,
            }
        }
    }

    pub fn unblock_response(&self) {
        let curr = self.current_snapshot.lock().last_included_index;
        let mut requests = self.snapshot_requests.lock();
        let mut processed = 0;
        for (index, condvar) in requests.iter() {
            if *index <= curr {
                processed += 1;
                condvar.notify_all();
            } else {
                break;
            }
        }
        requests.drain(0..processed);
    }
}

impl<T: DeserializeOwned> SnapshotHolder<T> {
    pub fn load_snapshot(&self, snapshot: Snapshot) -> T {
        let state = bincode::deserialize(&snapshot.data)
            .expect("Deserialization should never fail");
        *self.current_snapshot.lock() = snapshot;

        state
    }
}

use std::marker::PhantomData;

use parking_lot::Mutex;
use serde::Serialize;

use ruaft::Snapshot;
use serde::de::DeserializeOwned;

#[derive(Default)]
pub(crate) struct SnapshotHolder<T> {
    snapshot_requests: Mutex<Vec<usize>>,
    phantom: PhantomData<T>,
}

impl<T> SnapshotHolder<T> {
    pub fn request_snapshot(&self, min_index: usize) {
        let mut requests = self.snapshot_requests.lock();
        let pos = requests.binary_search(&min_index);
        if let Err(pos) = pos {
            requests.insert(pos, min_index);
        }
    }
}

impl<T: Serialize> SnapshotHolder<T> {
    pub fn take_snapshot(&self, state: &T, curr: usize) -> Option<Snapshot> {
        let requested = self
            .snapshot_requests
            .lock()
            .first()
            .map_or(false, |&min_index| min_index <= curr);

        if requested {
            let data = bincode::serialize(state)
                .expect("Serialization should never fail.");
            return Some(Snapshot {
                data,
                last_included_index: curr,
            });
        }
        None
    }

    pub fn unblock_response(&self, curr: usize) {
        let mut requests = self.snapshot_requests.lock();
        let mut processed = 0;
        for &index in requests.iter() {
            if index <= curr {
                processed += 1;
            } else {
                break;
            }
        }
        requests.drain(0..processed);
    }
}

impl<T: DeserializeOwned> SnapshotHolder<T> {
    pub fn load_snapshot(&self, snapshot: Snapshot) -> T {
        let state = bincode::deserialize(&snapshot.data).expect(&*format!(
            "Deserialization should never fail, {:?}",
            &snapshot.data
        ));

        state
    }
}

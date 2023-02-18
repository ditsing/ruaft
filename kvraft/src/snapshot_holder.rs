use std::marker::PhantomData;

use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::Serialize;

use ruaft::Snapshot;

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
        let mut requests = self.snapshot_requests.lock();

        let processed = requests.partition_point(|index| *index <= curr);
        if processed == 0 {
            return None;
        }

        requests.drain(0..processed);
        drop(requests);

        let data = bincode::serialize(state)
            .expect("Serialization should never fail.");
        Some(Snapshot {
            data,
            last_included_index: curr,
        })
    }
}

impl<T: DeserializeOwned> SnapshotHolder<T> {
    pub fn load_snapshot(&self, snapshot: Snapshot) -> T {
        if let Ok(result) = bincode::deserialize(&snapshot.data) {
            result
        } else {
            panic!("Deserialization should never fail, {:?}", snapshot.data)
        }
    }
}

use std::sync::Arc;

use parking_lot::Mutex;

#[derive(Clone)]
pub struct State {
    bytes: bytes::Bytes,
    snapshot: Vec<u8>,
}

pub struct MemoryPersister {
    state: Mutex<State>,
}

impl MemoryPersister {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                bytes: bytes::Bytes::new(),
                snapshot: vec![],
            }),
        }
    }
}

impl ruaft::Persister for MemoryPersister {
    fn read_state(&self) -> bytes::Bytes {
        self.state.lock().bytes.clone()
    }

    fn save_state(&self, data: bytes::Bytes) {
        self.state.lock().bytes = data;
    }

    fn state_size(&self) -> usize {
        self.state.lock().bytes.len()
    }

    fn save_snapshot_and_state(&self, state: bytes::Bytes, snapshot: &[u8]) {
        let mut this = self.state.lock();
        this.bytes = state;
        this.snapshot = snapshot.to_vec();
    }
}

impl MemoryPersister {
    pub fn read(&self) -> State {
        self.state.lock().clone()
    }

    pub fn restore(&self, state: State) {
        *self.state.lock() = state;
    }
}

#[derive(Default)]
pub struct MemoryStorage {
    state_vec: Vec<Arc<MemoryPersister>>,
}

impl MemoryStorage {
    pub fn make(&mut self) -> Arc<MemoryPersister> {
        let persister = Arc::new(MemoryPersister::new());
        self.state_vec.push(persister.clone());
        persister
    }

    pub fn at(&self, index: usize) -> Arc<MemoryPersister> {
        self.state_vec[index].clone()
    }

    pub fn replace(&mut self, index: usize) -> Arc<MemoryPersister> {
        let persister = Arc::new(MemoryPersister::new());
        self.state_vec[index] = persister.clone();
        persister
    }
}

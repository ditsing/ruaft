use parking_lot::Mutex;

#[derive(Clone)]
pub struct State {
    pub bytes: bytes::Bytes,
    pub snapshot: Vec<u8>,
}

pub struct Persister {
    state: Mutex<State>,
}

impl Persister {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                bytes: bytes::Bytes::new(),
                snapshot: vec![],
            }),
        }
    }
}

impl Default for Persister {
    fn default() -> Self {
        Self::new()
    }
}

impl ruaft::Persister for Persister {
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

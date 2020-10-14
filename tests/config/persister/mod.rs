use parking_lot::Mutex;

struct State {
    bytes: bytes::Bytes,
}

pub struct Persister {
    state: Mutex<State>,
}

impl Persister {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                bytes: bytes::Bytes::new(),
            }),
        }
    }
}

impl ruaft::Persister for Persister {
    fn read_state(&self) -> bytes::Bytes {
        self.state.lock().bytes.clone()
    }

    fn save_state(&self, data: bytes::Bytes) {
        self.state.lock().bytes = data;
    }
}

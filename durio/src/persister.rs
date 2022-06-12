#[derive(Default)]
pub struct DoNothingPersister {}

impl ruaft::Persister for DoNothingPersister {
    fn read_state(&self) -> bytes::Bytes {
        bytes::Bytes::new()
    }

    fn save_state(&self, _data: bytes::Bytes) {}

    fn state_size(&self) -> usize {
        0
    }

    fn save_snapshot_and_state(&self, _state: bytes::Bytes, _snapshot: &[u8]) {}
}

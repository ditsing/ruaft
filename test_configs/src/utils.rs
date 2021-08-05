use std::time::Duration;

pub fn sleep_millis(mills: u64) {
    std::thread::sleep(Duration::from_millis(mills))
}

pub const LONG_ELECTION_TIMEOUT_MILLIS: u64 = 1000;
pub fn sleep_election_timeouts(count: u64) {
    sleep_millis(LONG_ELECTION_TIMEOUT_MILLIS * count)
}

/// Pass this function to [`Raft::new`] if the application will not accept
/// any request for taking snapshots.
pub const NO_SNAPSHOT: fn(ruaft::Index) = |_| {};

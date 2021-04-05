use rand::{thread_rng, RngCore};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(
    Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize,
)]
struct UniqueId {
    clerk_id: u64,
    sequence_id: u64,
}

#[derive(Debug)]
struct UniqueIdSequence {
    clerk_id: u64,
    sequence_id: AtomicU64,
}

impl UniqueIdSequence {
    pub fn new() -> Self {
        Self {
            clerk_id: thread_rng().next_u64(),
            sequence_id: AtomicU64::new(0),
        }
    }

    pub fn zero(&self) -> UniqueId {
        UniqueId {
            clerk_id: self.clerk_id,
            sequence_id: 0,
        }
    }

    pub fn inc(&mut self) -> UniqueId {
        let seq = self.sequence_id.fetch_add(1, Ordering::Relaxed);
        UniqueId {
            clerk_id: self.clerk_id,
            sequence_id: seq,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum PutAppendEnum {
    Put,
    Append,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PutAppendArgs {
    key: String,
    value: String,
    op: PutAppendEnum,

    unique_id: UniqueId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum KvError {
    NoKey,
    Other(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PutAppendReply {
    result: Result<(), KvError>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetArgs {
    key: String,

    unique_id: UniqueId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetReply {
    result: Result<String, KvError>,
    is_retry: bool,
}

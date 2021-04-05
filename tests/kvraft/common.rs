use super::rand::RngCore;
pub use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
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
            clerk_id: rand::thread_rng().next_u64(),
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

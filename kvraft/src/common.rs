use std::sync::atomic::{AtomicU64, Ordering};

use rand::{thread_rng, RngCore};
use serde_derive::{Deserialize, Serialize};

pub type ClerkId = u64;
#[derive(
    Clone,
    Copy,
    Debug,
    Hash,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
)]
pub struct UniqueId {
    pub clerk_id: ClerkId,
    pub sequence_id: u64,
}

#[derive(Debug)]
pub struct UniqueIdSequence {
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

    pub fn inc(&self) -> UniqueId {
        let seq = self.sequence_id.fetch_add(1, Ordering::Relaxed);
        UniqueId {
            clerk_id: self.clerk_id,
            sequence_id: seq,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PutAppendEnum {
    Put,
    Append,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PutAppendArgs {
    pub key: String,
    pub value: String,
    pub op: PutAppendEnum,

    pub unique_id: UniqueId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PutAppendReply {
    pub result: Result<(), KVError>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetArgs {
    pub key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetReply {
    pub result: Result<Option<String>, KVError>,
}

#[derive(Clone, Debug, Default)]
pub struct KVRaftOptions {
    pub max_retry: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitSentinelArgs {
    pub unique_id: UniqueId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitSentinelReply {
    pub result: Result<(), KVError>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KVError {
    NotLeader,
    Expired,
    TimedOut,
    Conflict,
}

pub trait ValidReply {
    fn is_reply_valid(&self) -> bool;
}

impl<T> ValidReply for Result<T, KVError> {
    fn is_reply_valid(&self) -> bool {
        !matches!(
            self.as_ref().err(),
            Some(KVError::TimedOut) | Some(KVError::NotLeader)
        )
    }
}

impl ValidReply for PutAppendReply {
    fn is_reply_valid(&self) -> bool {
        self.result.is_reply_valid()
    }
}

impl ValidReply for GetReply {
    fn is_reply_valid(&self) -> bool {
        self.result.is_reply_valid()
    }
}

impl ValidReply for CommitSentinelReply {
    fn is_reply_valid(&self) -> bool {
        self.result.is_reply_valid()
    }
}

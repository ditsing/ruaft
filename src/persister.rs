use std::convert::TryFrom;

use bytes::Bytes;

use crate::{LogEntry, Peer, RaftState, Term};

pub trait Persister: Send + Sync {
    fn read_state(&self) -> Bytes;
    fn save_state(&self, bytes: Bytes);
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PersistedRaftState {
    pub current_term: Term,
    pub voted_for: Option<Peer>,
    pub log: Vec<LogEntry>,
}

impl<T: AsRef<RaftState>> From<T> for PersistedRaftState {
    fn from(raft_state: T) -> Self {
        Self::from(raft_state.as_ref())
    }
}

impl From<&RaftState> for PersistedRaftState {
    fn from(raft_state: &RaftState) -> Self {
        Self {
            current_term: raft_state.current_term,
            voted_for: raft_state.voted_for,
            log: raft_state.log.clone(),
        }
    }
}

impl TryFrom<Bytes> for PersistedRaftState {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(value.as_ref())
    }
}

impl Into<Bytes> for PersistedRaftState {
    fn into(self) -> Bytes {
        bincode::serialize(&self)
            .expect("Serialization should not fail")
            .into()
    }
}

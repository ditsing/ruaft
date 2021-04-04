use std::convert::TryFrom;

use bytes::Bytes;

use crate::{LogEntry, Peer, RaftState, Term};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait Persister: Send + Sync {
    fn read_state(&self) -> Bytes;
    fn save_state(&self, bytes: Bytes);
    fn state_size(&self) -> usize;

    fn save_snapshot_and_state(&self, state: Bytes, snapshot: &[u8]);
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PersistedRaftState<Command> {
    pub current_term: Term,
    pub voted_for: Option<Peer>,
    pub log: Vec<LogEntry<Command>>,
}

impl<Command: Clone, T: AsRef<RaftState<Command>>> From<T>
    for PersistedRaftState<Command>
{
    fn from(raft_state: T) -> Self {
        Self::from(raft_state.as_ref())
    }
}

impl<Command: Clone> From<&RaftState<Command>> for PersistedRaftState<Command> {
    fn from(raft_state: &RaftState<Command>) -> Self {
        Self {
            current_term: raft_state.current_term,
            voted_for: raft_state.voted_for,
            log: raft_state.log.all().to_vec(),
        }
    }
}

impl<Command: DeserializeOwned> TryFrom<Bytes> for PersistedRaftState<Command> {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(value.as_ref())
    }
}

impl<Command: Serialize> Into<Bytes> for PersistedRaftState<Command> {
    fn into(self) -> Bytes {
        bincode::serialize(&self)
            .expect("Serialization should not fail")
            .into()
    }
}

use serde_derive::{Deserialize, Serialize};

use crate::index_term::IndexTerm;
use crate::log_array::LogEntry;
use crate::raft::{Peer, Term};
use crate::Index;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub(crate) term: Term,
    pub(crate) candidate_id: Peer,
    pub(crate) last_log_index: Index,
    pub(crate) last_log_term: Term,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub(crate) term: Term,
    pub(crate) vote_granted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppendEntriesArgs<Command> {
    pub(crate) term: Term,
    pub(crate) leader_id: Peer,
    pub(crate) prev_log_index: Index,
    pub(crate) prev_log_term: Term,
    pub(crate) entries: Vec<LogEntry<Command>>,
    pub(crate) leader_commit: Index,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub(crate) term: Term,
    pub(crate) success: bool,
    pub(crate) committed: Option<IndexTerm>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallSnapshotArgs {
    pub(crate) term: Term,
    pub(crate) leader_id: Peer,
    pub(crate) last_included_index: Index,
    pub(crate) last_included_term: Term,
    // TODO(ditsing): Serde cannot handle Vec<u8> as efficient as expected.
    pub(crate) data: Vec<u8>,
    pub(crate) offset: usize,
    pub(crate) done: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub(crate) term: Term,
    pub(crate) committed: Option<IndexTerm>,
}

use crate::{
    log_array::LogArray, persister::PersistedRaftState, Index, Peer, Term,
};

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum State {
    Follower,
    Candidate,
    // TODO: add PreVote
    Leader,
}

pub(crate) struct RaftState<Command> {
    pub current_term: Term,
    pub voted_for: Option<Peer>,
    pub log: LogArray,

    pub commit_index: Index,
    pub last_applied: Index,

    pub next_index: Vec<Index>,
    pub match_index: Vec<Index>,
    pub current_step: Vec<i64>,

    pub state: State,

    pub leader_id: Peer,
}

impl<Command: Clone> RaftState<Command> {
    pub fn persisted_state(&self) -> PersistedRaftState<Command> {
        self.into()
    }
}

impl<Command> RaftState<Command> {
    pub fn is_leader(&self) -> bool {
        self.state == State::Leader
    }
}

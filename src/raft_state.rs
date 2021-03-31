use crate::{persister::PersistedRaftState, Index, LogEntry, Peer, Term};

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
    pub log: Vec<LogEntry<Command>>,

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
    pub fn last_log_index_and_term(&self) -> (Index, Term) {
        let len = self.log.len();
        assert!(len > 0, "There should always be at least one entry in log");
        (len - 1, self.log.last().unwrap().term)
    }

    pub fn is_leader(&self) -> bool {
        self.state == State::Leader
    }
}

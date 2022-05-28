use crate::{
    log_array::LogArray, persister::PersistedRaftState, Index, Peer, Term,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum State {
    Follower,
    Candidate,
    // TODO: add PreVote
    Leader,
}

pub(crate) struct RaftState<Command> {
    pub current_term: Term,
    pub voted_for: Option<Peer>,
    pub log: LogArray<Command>,

    pub commit_index: Index,
    pub last_applied: Index,

    pub next_index: Vec<Index>,
    pub match_index: Vec<Index>,
    pub current_step: Vec<i64>,

    pub state: State,

    pub leader_id: Peer,

    // Index of the first commit of each term as the leader.
    pub sentinel_commit_index: Index,
}

impl<Command> RaftState<Command> {
    pub fn create(peer_size: usize, me: Peer) -> Self {
        RaftState {
            current_term: Term(0),
            voted_for: None,
            log: crate::log_array::LogArray::create(),
            commit_index: 0,
            last_applied: 0,
            next_index: vec![1; peer_size],
            match_index: vec![0; peer_size],
            current_step: vec![0; peer_size],
            state: State::Follower,
            leader_id: me,
            sentinel_commit_index: 0,
        }
    }
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

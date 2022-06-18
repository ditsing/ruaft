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

    pub match_index: Vec<Index>,

    pub state: State,

    pub leader_id: Peer,
}

impl<Command> RaftState<Command> {
    pub fn create(peer_size: usize, me: Peer) -> Self {
        RaftState {
            current_term: Term(0),
            voted_for: None,
            log: LogArray::create(),
            commit_index: 0,
            last_applied: 0,
            match_index: vec![0; peer_size],
            state: State::Follower,
            leader_id: me,
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

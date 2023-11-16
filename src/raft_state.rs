use crate::{log_array::LogArray, Index, Peer, Term};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum State {
    Follower,
    Prevote,
    Candidate,
    Leader,
}

#[repr(align(64))]
pub(crate) struct RaftState<Command> {
    pub current_term: Term,
    pub voted_for: Option<Peer>,
    pub log: LogArray<Command>,

    pub commit_index: Index,

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
            match_index: vec![0; peer_size],
            state: State::Follower,
            leader_id: me,
        }
    }
}

impl<Command> RaftState<Command> {
    pub fn is_leader(&self) -> bool {
        self.state == State::Leader
    }
}

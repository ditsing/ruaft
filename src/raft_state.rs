use std::time::Instant;

use crate::{
    log_array::LogArray, persister::PersistedRaftState, Index, Peer, Term,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum State {
    Follower(FollowerData),
    Prevote,
    Candidate,
    Leader,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FollowerData {
    last_heartbeat: Option<Instant>,
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
            state: State::Follower(FollowerData::default()),
            leader_id: me,
        }
    }

    pub fn step_down(&mut self) {
        self.voted_for = None;
        self.state = State::Follower(FollowerData::default());
    }

    pub fn meet_leader(&mut self, leader_id: Peer) {
        self.leader_id = leader_id;
        self.state = State::Follower(FollowerData {
            last_heartbeat: Some(Instant::now()),
        })
    }

    pub fn last_heartbeat(&self) -> Option<Instant> {
        let State::Follower(follower_data) = &self.state else {
            return None;
        };
        return follower_data.last_heartbeat;
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

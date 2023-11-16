use std::sync::Arc;

use parking_lot::Mutex;

use crate::election::ElectionState;
use crate::storage::SharedLogPersister;
use crate::{RaftState, State, Term};

/// A closure that updates the `Term` of the `RaftState`.
#[derive(Clone)]
pub(crate) struct TermMarker<Command> {
    rf: Arc<Mutex<RaftState<Command>>>,
    election: Arc<ElectionState>,
    persister: SharedLogPersister<Command>,
}

impl<Command> TermMarker<Command> {
    /// Create a `TermMarker` that can be passed to async tasks.
    pub fn create(
        rf: Arc<Mutex<RaftState<Command>>>,
        election: Arc<ElectionState>,
        persister: SharedLogPersister<Command>,
    ) -> Self {
        Self {
            rf,
            election,
            persister,
        }
    }

    pub fn mark(&self, term: Term) {
        let mut rf = self.rf.lock();
        if term > rf.current_term {
            rf.current_term = term;
            rf.voted_for = None;
            rf.state = State::Follower;

            self.election.reset_election_timer();
            self.persister.save_term_vote(&rf);
        }
    }
}

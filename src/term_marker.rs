use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

use crate::election::ElectionState;
use crate::{Persister, RaftState, State, Term};

/// A closure that updates the `Term` of the `RaftState`.
#[derive(Clone)]
pub(crate) struct TermMarker<Command> {
    rf: Arc<Mutex<RaftState<Command>>>,
    election: Arc<ElectionState>,
    persister: Arc<dyn Persister>,
}

impl<Command: Clone + Serialize> TermMarker<Command> {
    /// Create a `TermMarker` that can be passed to async tasks.
    pub fn create(
        rf: Arc<Mutex<RaftState<Command>>>,
        election: Arc<ElectionState>,
        persister: Arc<dyn Persister>,
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
            self.persister.save_state(rf.persisted_state().into());
        }
    }
}

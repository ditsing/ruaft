use crate::{Raft, RequestVoteArgs, RequestVoteReply, State};

// Command must be
// 1. clone: they are copied to the persister.
// 2. serialize: they are converted to bytes to persist.
// 3. default: a default value is used as the first element of the log.
impl<Command> Raft<Command>
where
    Command: Clone + serde::Serialize + Default,
{
    pub(crate) fn process_request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> RequestVoteReply {
        // Note: do not change this to `let _ = ...`.
        let _guard = self.daemon_env.for_scope();

        let mut rf = self.inner_state.lock();

        let term = rf.current_term;
        #[allow(clippy::comparison_chain)]
        if args.term < term {
            return RequestVoteReply {
                term,
                vote_granted: false,
            };
        } else if args.term > term {
            rf.current_term = args.term;
            rf.voted_for = None;
            rf.state = State::Follower;

            self.election.reset_election_timer();
            self.persister.save_state(rf.persisted_state().into());
        }

        let voted_for = rf.voted_for;
        let last_log = rf.log.last_index_term();
        if (voted_for.is_none() || voted_for == Some(args.candidate_id))
            && (args.last_log_term > last_log.term
                || (args.last_log_term == last_log.term
                    && args.last_log_index >= last_log.index))
        {
            rf.voted_for = Some(args.candidate_id);

            // It is possible that we have set a timer above when updating the
            // current term. It does not hurt to update the timer again.
            // We do need to persist, though.
            self.election.reset_election_timer();
            self.persister.save_state(rf.persisted_state().into());

            RequestVoteReply {
                term: args.term,
                vote_granted: true,
            }
        } else {
            RequestVoteReply {
                term: args.term,
                vote_granted: false,
            }
        }
    }
}

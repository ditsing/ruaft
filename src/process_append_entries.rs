use crate::daemon_env::ErrorKind;
use crate::{
    check_or_record, AppendEntriesArgs, AppendEntriesReply, IndexTerm, Raft,
    State,
};

// Command must be
// 1. clone: they are copied to the persister.
// 2. serialize: they are converted to bytes to persist.
impl<Command: Clone + serde::Serialize> Raft<Command> {
    pub fn process_append_entries(
        &self,
        args: AppendEntriesArgs<Command>,
    ) -> AppendEntriesReply {
        // Note: do not change this to `let _ = ...`.
        let _guard = self.daemon_env.for_scope();

        let mut rf = self.inner_state.lock();
        if rf.current_term > args.term {
            return AppendEntriesReply {
                term: rf.current_term,
                success: false,
                committed: Some(rf.log.first_after(rf.commit_index).into()),
            };
        }

        if rf.current_term < args.term {
            rf.current_term = args.term;
            rf.voted_for = None;
            self.persister.save_state(rf.persisted_state().into());
        }

        rf.state = State::Follower;
        rf.leader_id = args.leader_id;

        self.election.reset_election_timer();

        if rf.log.start() > args.prev_log_index
            || rf.log.end() <= args.prev_log_index
            || rf.log.at(args.prev_log_index).term != args.prev_log_term
        {
            return AppendEntriesReply {
                term: args.term,
                success: args.prev_log_index < rf.log.start(),
                committed: Some(rf.log.first_after(rf.commit_index).into()),
            };
        }
        let index_matches = args
            .entries
            .iter()
            .enumerate()
            .all(|(i, entry)| entry.index == i + args.prev_log_index + 1);
        if !index_matches {
            let indexes: Vec<IndexTerm> =
                args.entries.iter().map(|e| e.into()).collect();

            check_or_record!(
                index_matches,
                ErrorKind::AppendEntriesIndexMismatch(
                    args.prev_log_index,
                    indexes
                ),
                "Entries in AppendEntries request shows index mismatch",
                &rf
            );

            return AppendEntriesReply {
                term: args.term,
                success: false,
                committed: Some(rf.log.first_after(rf.commit_index).into()),
            };
        }

        // COMMIT_INDEX_INVARIANT: Before this loop, we can safely assume that
        // commit_index < log.end().
        for (i, entry) in args.entries.iter().enumerate() {
            let index = i + args.prev_log_index + 1;
            if rf.log.end() > index {
                if rf.log.at(index).term != entry.term {
                    check_or_record!(
                        index > rf.commit_index,
                        ErrorKind::RollbackCommitted(index),
                        "Entries before commit index should never be rolled back",
                        &rf
                    );
                    // COMMIT_INDEX_INVARIANT: log.end() shrinks to index. We
                    // checked that index is strictly larger than commit_index.
                    // The condition that commit_index < log.end() holds.
                    rf.log.truncate(index);
                    rf.log.push(entry.clone());
                }
                // COMMIT_INDEX_INVARIANT: Otherwise log.end() does not move.
                // The condition that commit_index < log.end() holds.
            } else {
                // COMMIT_INDEX_INVARIANT: log.end() grows larger. The condition
                // that commit_index < log.end() holds.
                rf.log.push(entry.clone());
            }
        }
        // COMMIT_INDEX_INVARIANT: After the loop, commit_index < log.end()
        // must still hold.

        self.persister.save_state(rf.persisted_state().into());

        // SNAPSHOT_INDEX_INVARIANT: commit_index increases (or stays unchanged)
        // after this if-statement. log.start() <= commit_index still holds.
        if args.leader_commit > rf.commit_index {
            // COMMIT_INDEX_INVARIANT: commit_index < log.end() still holds
            // after this assignment.
            rf.commit_index = if args.leader_commit < rf.log.end() {
                // COMMIT_INDEX_INVARIANT: The if-condition guarantees that
                // leader_commit < log.end().
                args.leader_commit
            } else {
                // COMMIT_INDEX_INVARIANT: This is exactly log.end() - 1.
                rf.log.last_index_term().index
            };
            self.apply_command_signal.notify_one();
        }
        self.snapshot_daemon.log_grow(rf.log.start(), rf.log.end());

        AppendEntriesReply {
            term: args.term,
            success: true,
            committed: None,
        }
    }
}

use crate::check_or_record;
use crate::daemon_env::ErrorKind;
use crate::{InstallSnapshotArgs, InstallSnapshotReply, Raft};

impl<C: Clone + serde::Serialize> Raft<C> {
    pub fn process_install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> InstallSnapshotReply {
        // Note: do not change this to `let _ = ...`.
        let _guard = self.daemon_env.for_scope();

        if args.offset != 0 || !args.done {
            panic!("Current implementation cannot handle segmented snapshots.")
        }

        let mut rf = self.inner_state.lock();
        if rf.current_term > args.term {
            return InstallSnapshotReply {
                term: rf.current_term,
                committed: None,
            };
        }

        if rf.current_term < args.term {
            rf.current_term = args.term;
            rf.voted_for = None;
            self.persister.save_state(rf.persisted_state().into());
        }

        rf.meet_leader(args.leader_id);
        self.election.reset_election_timer();

        // The above code is exactly the same as AppendEntries.

        // The snapshot could not be verified because the index is beyond log
        // start. Fail this request and ask leader to send something that we
        // could verify. We cannot rollback to a point beyond commit index
        // anyway, let alone rolling back before the log start(). If we rollback
        // and the system fails right after the rollback, committed entries
        // before log start would be lost forever.
        //
        // The commit index is sent back to leader. The leader would never need
        // to rollback beyond that, since it is guaranteed that committed log
        // entries will never be rolled back.
        if args.last_included_index < rf.log.start() {
            // This is a condition that should always hold. Combine the if- and
            // assert- conditions, we can conclude that last_include_index is
            // smaller than commit_index. The leader might double check that.
            assert!(rf.log.start() <= rf.commit_index);
            return InstallSnapshotReply {
                term: args.term,
                committed: Some(rf.log.first_after(rf.commit_index).into()),
            };
        }

        if args.last_included_index < rf.log.end()
            && args.last_included_index >= rf.log.start()
            && args.last_included_term
                == rf.log.at(args.last_included_index).term
        {
            // Do nothing if the index and term match the current snapshot.
            if args.last_included_index != rf.log.start() {
                // SNAPSHOT_INDEX_INVARIANT: commit_index increases (or stays
                // unchanged) after this if-statement. Thus log.start() <=
                // commit_index still holds.
                // COMMIT_INDEX_INVARIANT: The condition of the outer if-
                // statement guarantees that last_included_index is smaller
                // than log.end().
                if rf.commit_index < args.last_included_index {
                    rf.commit_index = args.last_included_index;
                }
                rf.log.shift(args.last_included_index, args.data);
            }
            // COMMIT_INDEX_INVARIANT: log.end() does not move.
        } else {
            check_or_record!(
                args.last_included_index > rf.commit_index,
                ErrorKind::SnapshotBeforeCommitted(
                    args.last_included_index,
                    args.last_included_term
                ),
                "Snapshot data is inconsistent with committed log entry.",
                &rf
            );
            // COMMIT_INDEX_INVARIANT, SNAPSHOT_INDEX_INVARIANT: After those two
            // updates, commit_index is exactly log.start() and log.end() - 1.
            rf.commit_index = args.last_included_index;
            rf.log.reset(
                args.last_included_index,
                args.last_included_term,
                args.data,
            );
        }

        self.persister.save_snapshot_and_state(
            rf.persisted_state().into(),
            rf.log.snapshot().1,
        );

        self.apply_command_signal.notify_one();
        InstallSnapshotReply {
            term: args.term,
            committed: None,
        }
    }
}

use crate::daemon_env::ErrorKind;
use crate::{
    check_or_record, AppendEntriesArgs, AppendEntriesReply, Raft, State,
};

// Command must be
// 1. clone: they are copied to the persister.
// 2. serialize: they are converted to bytes to persist.
// 3. default: a default value is used as the first element of the log.
impl<Command> Raft<Command>
where
    Command: Clone + serde::Serialize + Default,
{
    pub(crate) fn process_append_entries(
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
            || rf.log[args.prev_log_index].term != args.prev_log_term
        {
            return AppendEntriesReply {
                term: args.term,
                success: args.prev_log_index < rf.log.start(),
                committed: Some(rf.log.first_after(rf.commit_index).into()),
            };
        }

        for (i, entry) in args.entries.iter().enumerate() {
            let index = i + args.prev_log_index + 1;
            if rf.log.end() > index {
                if rf.log[index].term != entry.term {
                    check_or_record!(
                        index > rf.commit_index,
                        ErrorKind::RollbackCommitted(index),
                        "Entries before commit index should never be rolled back",
                        &rf
                    );
                    rf.log.truncate(index);
                    rf.log.push(entry.clone());
                }
            } else {
                rf.log.push(entry.clone());
            }
        }

        self.persister.save_state(rf.persisted_state().into());

        if args.leader_commit > rf.commit_index {
            rf.commit_index = if args.leader_commit < rf.log.end() {
                args.leader_commit
            } else {
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

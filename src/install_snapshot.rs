use crate::{Index, Peer, Raft, RaftState, State, Term};
use parking_lot::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct InstallSnapshotArgs {
    term: Term,
    leader_id: Peer,
    last_included_index: Index,
    last_included_term: Term,
    // TODO(ditsing): this seems less efficient.
    data: Vec<u8>,
    offset: usize,
    done: bool,
}

pub(crate) struct InstallSnapshotReply {
    term: Term,
}

impl<C: Clone + Default + serde::Serialize> Raft<C> {
    pub(crate) fn process_install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> InstallSnapshotReply {
        if args.offset != 0 || !args.done {
            panic!("Current implementation cannot handle segmented snapshots.")
        }

        let mut rf = self.inner_state.lock();
        if rf.current_term > args.term {
            return InstallSnapshotReply {
                term: rf.current_term,
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

        // The above code is exactly the same as AppendEntries.
        if args.last_included_index < rf.log.end()
            && args.last_included_index >= rf.log.start()
            && args.last_included_term == rf.log[args.last_included_index].term
        {
            rf.log.shift(args.last_included_index, args.data);
        } else {
            rf.log.reset(
                args.last_included_index,
                args.last_included_term,
                args.data,
            );
        }
        // The length of the log might shrink.
        let last_log_index = rf.log.last_index_term().index;
        if rf.commit_index > last_log_index {
            rf.commit_index = last_log_index;
        }
        self.persister.save_state(bytes::Bytes::new()); // TODO(ditsing)

        self.apply_command_signal.notify_one();
        InstallSnapshotReply { term: args.term }
    }

    fn build_install_snapshot(
        rf: &Mutex<RaftState<C>>,
    ) -> Option<InstallSnapshotArgs> {
        let rf = rf.lock();
        if !rf.is_leader() {
            return None;
        }
        let (last, snapshot) = rf.log.snapshot();
        Some(InstallSnapshotArgs {
            term: rf.current_term,
            leader_id: rf.leader_id,
            last_included_index: last.index,
            last_included_term: last.term,
            data: snapshot.to_owned(),
            offset: 0,
            done: true,
        })
    }
}

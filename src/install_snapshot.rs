use crate::index_term::IndexTerm;
use crate::utils::retry_rpc;
use crate::{
    Index, Peer, Raft, RaftState, RpcClient, State, SyncLogEntryResult, Term,
    RPC_DEADLINE,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct InstallSnapshotArgs {
    pub(crate) term: Term,
    leader_id: Peer,
    pub(crate) last_included_index: Index,
    last_included_term: Term,
    // TODO(ditsing): Serde cannot handle Vec<u8> as efficient as expected.
    data: Vec<u8>,
    offset: usize,
    done: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct InstallSnapshotReply {
    term: Term,
    committed: Option<IndexTerm>,
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
                committed: None,
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

        // The snapshot could not be verified because the index is beyond log
        // start. Fail this request and ask leader to send something that we
        // could verify. We cannot rollback to a point beyond commit index
        // anyway. Otherwise if the system fails right after the rollback,
        // committed entries before log start would be lost forever.
        //
        // The commit index is sent back to leader. The leader would never need
        // to rollback beyond that, since it is guaranteed that committed log
        // entries will never be rolled back.
        if args.last_included_index < rf.log.start() {
            return InstallSnapshotReply {
                term: args.term,
                committed: Some(rf.log.first_after(rf.commit_index).into()),
            };
        }

        if args.last_included_index < rf.log.end()
            && args.last_included_index >= rf.log.start()
            && args.last_included_term == rf.log[args.last_included_index].term
        {
            // Do nothing if the index and term match the current snapshot.
            if args.last_included_index != rf.log.start() {
                if rf.commit_index < args.last_included_index {
                    rf.commit_index = args.last_included_index;
                }
                rf.log.shift(args.last_included_index, args.data);
            }
        } else {
            assert!(args.last_included_index > rf.commit_index);
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

    pub(crate) fn build_install_snapshot(
        rf: &RaftState<C>,
    ) -> InstallSnapshotArgs {
        let (last, snapshot) = rf.log.snapshot();
        InstallSnapshotArgs {
            term: rf.current_term,
            leader_id: rf.leader_id,
            last_included_index: last.index,
            last_included_term: last.term,
            data: snapshot.to_owned(),
            offset: 0,
            done: true,
        }
    }

    const INSTALL_SNAPSHOT_RETRY: usize = 1;
    pub(crate) async fn send_install_snapshot(
        rpc_client: &RpcClient,
        args: InstallSnapshotArgs,
    ) -> std::io::Result<SyncLogEntryResult> {
        let term = args.term;
        let reply = retry_rpc(
            Self::INSTALL_SNAPSHOT_RETRY,
            RPC_DEADLINE,
            move |_round| rpc_client.call_install_snapshot(args.clone()),
        )
        .await?;
        Ok(if reply.term == term {
            if let Some(committed) = reply.committed {
                SyncLogEntryResult::Archived(committed)
            } else {
                SyncLogEntryResult::Success
            }
        } else {
            SyncLogEntryResult::TermElapsed(reply.term)
        })
    }
}

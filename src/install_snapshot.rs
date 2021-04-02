use crate::utils::retry_rpc;
use crate::{
    Index, Peer, Raft, RaftState, RpcClient, State, Term, RPC_DEADLINE,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct InstallSnapshotArgs {
    pub(crate) term: Term,
    leader_id: Peer,
    pub(crate) last_included_index: Index,
    last_included_term: Term,
    // TODO(ditsing): this seems less efficient.
    data: Vec<u8>,
    offset: usize,
    done: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
    ) -> std::io::Result<Option<bool>> {
        let term = args.term;
        let reply = retry_rpc(
            Self::INSTALL_SNAPSHOT_RETRY,
            RPC_DEADLINE,
            move |_round| rpc_client.call_install_snapshot(args.clone()),
        )
        .await?;
        Ok(if reply.term == term { Some(true) } else { None })
    }
}

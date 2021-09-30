use std::sync::Arc;

use tarpc::context::Context;

use kvraft::UniqueKVOp;
use ruaft::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs,
    InstallSnapshotReply, Raft, RequestVoteArgs, RequestVoteReply,
};

#[tarpc::service]
trait RuaftSerivce {
    async fn append_entries(
        args: AppendEntriesArgs<UniqueKVOp>,
    ) -> AppendEntriesReply;
    async fn install_snapshot(
        args: InstallSnapshotArgs,
    ) -> InstallSnapshotReply;
    async fn request_vote(args: RequestVoteArgs) -> RequestVoteReply;
}

struct RuaftRpcServer(Arc<Raft<UniqueKVOp>>);

#[tarpc::server]
impl RuaftSerivce for RuaftRpcServer {
    async fn append_entries(
        self,
        _context: Context,
        args: AppendEntriesArgs<UniqueKVOp>,
    ) -> AppendEntriesReply {
        self.0.process_append_entries(args)
    }

    async fn install_snapshot(
        self,
        _context: Context,
        args: InstallSnapshotArgs,
    ) -> InstallSnapshotReply {
        self.0.process_install_snapshot(args)
    }

    async fn request_vote(
        self,
        _context: Context,
        args: RequestVoteArgs,
    ) -> RequestVoteReply {
        self.0.process_request_vote(args)
    }
}

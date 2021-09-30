use std::sync::Arc;

use async_trait::async_trait;
use tarpc::context::Context;

use kvraft::UniqueKVOp;
use ruaft::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs,
    InstallSnapshotReply, Raft, RemoteRaft, RequestVoteArgs, RequestVoteReply,
};
use std::io::ErrorKind;
use tarpc::client::RpcError;

#[tarpc::service]
trait RaftService {
    async fn append_entries(
        args: AppendEntriesArgs<UniqueKVOp>,
    ) -> AppendEntriesReply;
    async fn install_snapshot(
        args: InstallSnapshotArgs,
    ) -> InstallSnapshotReply;
    async fn request_vote(args: RequestVoteArgs) -> RequestVoteReply;
}

struct RaftRpcServer(Arc<Raft<UniqueKVOp>>);

#[tarpc::server]
impl RaftService for RaftRpcServer {
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

#[async_trait]
impl RemoteRaft<UniqueKVOp> for RaftServiceClient {
    async fn request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> std::io::Result<RequestVoteReply> {
        self.request_vote(Context::current(), args)
            .await
            .map_err(translate_rpc_error)
    }

    async fn append_entries(
        &self,
        args: AppendEntriesArgs<UniqueKVOp>,
    ) -> std::io::Result<AppendEntriesReply> {
        self.append_entries(Context::current(), args)
            .await
            .map_err(translate_rpc_error)
    }

    async fn install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> std::io::Result<InstallSnapshotReply> {
        self.install_snapshot(Context::current(), args)
            .await
            .map_err(translate_rpc_error)
    }
}

fn translate_rpc_error(e: RpcError) -> std::io::Error {
    match e {
        RpcError::Disconnected => std::io::Error::new(ErrorKind::BrokenPipe, e),
        RpcError::DeadlineExceeded => {
            std::io::Error::new(ErrorKind::TimedOut, e)
        }
        RpcError::Server(server_error) => {
            std::io::Error::new(ErrorKind::Other, server_error)
        }
    }
}

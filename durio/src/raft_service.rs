use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tarpc::context::Context;

use kvraft::UniqueKVOp;
use ruaft::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs,
    InstallSnapshotReply, Raft, RemoteRaft, RequestVoteArgs, RequestVoteReply,
};

#[tarpc::service]
pub(crate) trait RaftService {
    async fn append_entries(
        args: AppendEntriesArgs<UniqueKVOp>,
    ) -> AppendEntriesReply;
    async fn install_snapshot(
        args: InstallSnapshotArgs,
    ) -> InstallSnapshotReply;
    async fn request_vote(args: RequestVoteArgs) -> RequestVoteReply;
}

#[derive(Clone)]
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

pub(crate) struct OptionalRaftServiceClient(Option<RaftServiceClient>);

#[async_trait]
impl RemoteRaft<UniqueKVOp> for OptionalRaftServiceClient {
    async fn request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> std::io::Result<RequestVoteReply> {
        self.0
            .as_ref()
            .unwrap()
            .request_vote(Context::current(), args)
            .await
            .map_err(crate::utils::translate_rpc_error)
    }

    async fn append_entries(
        &self,
        args: AppendEntriesArgs<UniqueKVOp>,
    ) -> std::io::Result<AppendEntriesReply> {
        self.0
            .as_ref()
            .unwrap()
            .append_entries(Context::current(), args)
            .await
            .map_err(crate::utils::translate_rpc_error)
    }

    async fn install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> std::io::Result<InstallSnapshotReply> {
        self.0
            .as_ref()
            .unwrap()
            .install_snapshot(Context::current(), args)
            .await
            .map_err(crate::utils::translate_rpc_error)
    }
}

pub(crate) fn no_raft_service() -> OptionalRaftServiceClient {
    OptionalRaftServiceClient(None)
}

pub(crate) async fn connect_to_raft_service(
    addr: SocketAddr,
) -> std::io::Result<OptionalRaftServiceClient> {
    let conn = tarpc::serde_transport::tcp::connect(
        addr,
        tokio_serde::formats::Json::default,
    )
    .await?;
    let client =
        RaftServiceClient::new(tarpc::client::Config::default(), conn).spawn();
    Ok(OptionalRaftServiceClient(Some(client)))
}

pub(crate) fn start_raft_service_server(
    addr: SocketAddr,
    raft: Arc<Raft<UniqueKVOp>>,
) -> impl Future<Output = std::io::Result<()>> {
    let server = RaftRpcServer(raft);
    crate::utils::start_tarpc_server(addr, server.serve())
}

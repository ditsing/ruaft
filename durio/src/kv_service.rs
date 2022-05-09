use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tarpc::context::Context;

use kvraft::{
    CommitSentinelArgs, CommitSentinelReply, GetArgs, GetReply, KVServer,
    PutAppendArgs, PutAppendReply, RemoteKvraft,
};

#[tarpc::service]
pub(crate) trait KVService {
    async fn get(args: GetArgs) -> GetReply;
    async fn put_append(args: PutAppendArgs) -> PutAppendReply;
    async fn commit_sentinel(args: CommitSentinelArgs) -> CommitSentinelReply;
}

#[derive(Clone)]
struct KVRpcServer(Arc<KVServer>);

#[tarpc::server]
impl KVService for KVRpcServer {
    async fn get(self, _context: Context, args: GetArgs) -> GetReply {
        self.0.get(args).await
    }

    async fn put_append(
        self,
        _context: Context,
        args: PutAppendArgs,
    ) -> PutAppendReply {
        self.0.put_append(args).await
    }

    async fn commit_sentinel(
        self,
        _context: Context,
        args: CommitSentinelArgs,
    ) -> CommitSentinelReply {
        self.0.commit_sentinel(args).await
    }
}

#[async_trait]
impl RemoteKvraft for KVServiceClient {
    async fn get(&self, args: GetArgs) -> std::io::Result<GetReply> {
        self.get(Context::current(), args)
            .await
            .map_err(crate::utils::translate_rpc_error)
    }

    async fn put_append(
        &self,
        args: PutAppendArgs,
    ) -> std::io::Result<PutAppendReply> {
        self.put_append(Context::current(), args)
            .await
            .map_err(crate::utils::translate_rpc_error)
    }

    async fn commit_sentinel(
        &self,
        args: CommitSentinelArgs,
    ) -> std::io::Result<CommitSentinelReply> {
        self.commit_sentinel(Context::current(), args)
            .await
            .map_err(crate::utils::translate_rpc_error)
    }
}

#[allow(dead_code)]
pub(crate) async fn connect_to_kv_service(
    addr: SocketAddr,
) -> std::io::Result<KVServiceClient> {
    let conn = tarpc::serde_transport::tcp::connect(
        addr,
        tokio_serde::formats::Json::default,
    )
    .await?;
    let client =
        KVServiceClient::new(tarpc::client::Config::default(), conn).spawn();
    Ok(client)
}

pub(crate) fn start_kv_service_server(
    addr: SocketAddr,
    kv_server: Arc<KVServer>,
) -> impl Future<Output = std::io::Result<()>> {
    let server = KVRpcServer(kv_server);
    crate::utils::start_tarpc_server(addr, server.serve())
}

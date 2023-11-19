use std::future::Future;

use async_trait::async_trait;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use labrpc::{Client, Network, ReplyMessage, RequestMessage, Server};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::Serialize;

use kvraft::{
    CommitSentinelArgs, CommitSentinelReply, GetArgs, GetReply, KVServer,
    PutAppendArgs, PutAppendReply, RemoteKvraft,
};
use raft::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs,
    InstallSnapshotReply, Raft, ReplicableCommand, RequestVoteArgs,
    RequestVoteReply,
};

const REQUEST_VOTE_RPC: &str = "Raft.RequestVote";
const APPEND_ENTRIES_RPC: &str = "Raft.AppendEntries";
const INSTALL_SNAPSHOT_RPC: &str = "Raft.InstallSnapshot";

pub struct RpcClient(Client);

impl RpcClient {
    pub fn new(client: Client) -> Self {
        Self(client)
    }

    async fn call_rpc<Method, Request, Reply>(
        &self,
        method: Method,
        request: Request,
    ) -> std::io::Result<Reply>
    where
        Method: AsRef<str>,
        Request: Serialize,
        Reply: DeserializeOwned,
    {
        let data = RequestMessage::from(
            bincode::serialize(&request)
                .expect("Serialization of requests should not fail"),
        );

        let reply = self.0.call_rpc(method.as_ref().to_owned(), data).await?;

        Ok(bincode::deserialize(reply.as_ref())
            .expect("Deserialization of reply should not fail"))
    }
}

#[async_trait]
impl<Command: ReplicableCommand> raft::RemoteRaft<Command> for RpcClient {
    async fn request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> std::io::Result<RequestVoteReply> {
        self.call_rpc(REQUEST_VOTE_RPC, args).await
    }

    async fn append_entries(
        &self,
        args: AppendEntriesArgs<Command>,
    ) -> std::io::Result<AppendEntriesReply> {
        self.call_rpc(APPEND_ENTRIES_RPC, args).await
    }

    async fn install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> std::io::Result<InstallSnapshotReply> {
        self.call_rpc(INSTALL_SNAPSHOT_RPC, args).await
    }
}

const GET: &str = "KVServer.Get";
const PUT_APPEND: &str = "KVServer.PutAppend";
const COMMIT_SENTINEL: &str = "KVServer.CommitSentinel";

#[async_trait]
impl RemoteKvraft for RpcClient {
    async fn get(&self, args: GetArgs) -> std::io::Result<GetReply> {
        self.call_rpc(GET, args).await
    }

    async fn put_append(
        &self,
        args: PutAppendArgs,
    ) -> std::io::Result<PutAppendReply> {
        self.call_rpc(PUT_APPEND, args).await
    }

    async fn commit_sentinel(
        &self,
        args: CommitSentinelArgs,
    ) -> std::io::Result<CommitSentinelReply> {
        self.call_rpc(COMMIT_SENTINEL, args).await
    }
}

pub fn make_rpc_handler<Request, Reply, F>(
    func: F,
) -> impl Fn(RequestMessage) -> ReplyMessage
where
    Request: DeserializeOwned,
    Reply: Serialize,
    F: 'static + Fn(Request) -> Reply,
{
    move |request| {
        let reply = func(
            bincode::deserialize(&request)
                .expect("Deserialization should not fail"),
        );

        ReplyMessage::from(
            bincode::serialize(&reply).expect("Serialization should not fail"),
        )
    }
}

pub fn make_async_rpc_handler<'a, Request, Reply, F, Fut>(
    func: F,
) -> impl Fn(RequestMessage) -> BoxFuture<'a, ReplyMessage>
where
    Request: DeserializeOwned + Send,
    Reply: Serialize,
    Fut: Future<Output = Reply> + Send + 'a,
    F: 'a + Send + Clone + FnOnce(Request) -> Fut,
{
    move |request| {
        let func = func.clone();
        let fut = async move {
            let request = bincode::deserialize(&request)
                .expect("Deserialization should not fail");
            let reply = func(request).await;
            ReplyMessage::from(
                bincode::serialize(&reply)
                    .expect("Serialization should not fail"),
            )
        };
        fut.boxed()
    }
}

pub fn register_server<
    Command: 'static + Clone + Send + Serialize + DeserializeOwned,
    S: AsRef<str>,
>(
    raft: Raft<Command>,
    name: S,
    network: &Mutex<Network>,
) -> std::io::Result<()> {
    let mut network = network.lock();
    let server_name = name.as_ref();
    let mut server = Server::make_server(server_name);

    server.register_rpc_handler(REQUEST_VOTE_RPC.to_owned(), {
        let raft = raft.clone();
        make_rpc_handler(move |args| raft.process_request_vote(args))
    })?;

    server.register_rpc_handler(APPEND_ENTRIES_RPC.to_owned(), {
        let raft = raft.clone();
        make_rpc_handler(move |args| raft.process_append_entries(args))
    })?;

    server.register_rpc_handler(
        INSTALL_SNAPSHOT_RPC.to_owned(),
        make_rpc_handler(move |args| raft.process_install_snapshot(args)),
    )?;

    network.add_server(server_name, server);

    Ok(())
}
pub fn register_kv_server<
    KV: 'static + AsRef<KVServer> + Send + Sync + Clone,
    S: AsRef<str>,
>(
    kv: KV,
    name: S,
    network: &Mutex<Network>,
) -> std::io::Result<()> {
    let mut network = network.lock();
    let server_name = name.as_ref();
    let mut server = Server::make_server(server_name);

    server.register_async_rpc_handler(GET.to_owned(), {
        let kv = kv.clone();
        // Note: make_async_rpc_handler expects a FnOnce.
        make_async_rpc_handler(move |args| async move {
            kv.as_ref().get(args).await
        })
    })?;

    server.register_async_rpc_handler(PUT_APPEND.to_owned(), {
        let kv = kv.clone();
        make_async_rpc_handler(move |args| async move {
            kv.as_ref().put_append(args).await
        })
    })?;

    server.register_async_rpc_handler(
        COMMIT_SENTINEL.to_owned(),
        make_async_rpc_handler(move |args| async move {
            kv.as_ref().commit_sentinel(args).await
        }),
    )?;

    network.add_server(server_name, server);

    Ok(())
}

#[cfg(test)]
mod tests {
    use raft::utils::do_nothing::DoNothingRaftStorage;
    use raft::utils::integration_test::{
        make_append_entries_args, make_request_vote_args,
        unpack_append_entries_reply, unpack_request_vote_reply,
    };
    use raft::{ApplyCommandMessage, RemoteRaft, Term};

    use super::*;

    #[test]
    fn test_basic_message() -> std::io::Result<()> {
        test_utils::init_test_log!();

        let client = {
            let network = Network::run_daemon();
            let name = "test-basic-message";

            let client = network
                .lock()
                .make_client("test-basic-message", name.to_owned());

            let raft = Raft::new(
                vec![RpcClient(client)],
                0,
                DoNothingRaftStorage,
                |_: ApplyCommandMessage<i32>| {},
                crate::utils::NO_SNAPSHOT,
            );
            register_server(raft, name, network.as_ref())?;

            let client = network
                .lock()
                .make_client("test-basic-message", name.to_owned());
            client
        };

        let rpc_client = RpcClient(client);
        let request = make_request_vote_args(Term(2021), 0, 0, Term(0));
        let response = futures::executor::block_on(
            (&rpc_client as &dyn RemoteRaft<i32>).request_vote(request),
        )?;
        let (_, vote_granted) = unpack_request_vote_reply(response);
        assert!(vote_granted);

        let request =
            make_append_entries_args::<i32>(Term(2021), 0, 0, Term(0), 0);
        let response =
            futures::executor::block_on(rpc_client.append_entries(request))?;
        let (Term(term), success) = unpack_append_entries_reply(response);
        assert_eq!(2021, term);
        assert!(success);

        Ok(())
    }
}

use async_trait::async_trait;
use labrpc::{Client, Network, ReplyMessage, RequestMessage, Server};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::Serialize;

use kvraft::{
    GetArgs, GetReply, KVServer, PutAppendArgs, PutAppendReply, RemoteKvraft,
};
use ruaft::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs,
    InstallSnapshotReply, Raft, RequestVoteArgs, RequestVoteReply,
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
impl<Command: 'static + Send + Serialize> ruaft::RemoteRaft<Command>
    for RpcClient
{
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
}

pub fn make_rpc_handler<Request, Reply, F>(
    func: F,
) -> Box<dyn Fn(RequestMessage) -> ReplyMessage>
where
    Request: DeserializeOwned,
    Reply: Serialize,
    F: 'static + Fn(Request) -> Reply,
{
    Box::new(move |request| {
        let reply = func(
            bincode::deserialize(&request)
                .expect("Deserialization should not fail"),
        );

        ReplyMessage::from(
            bincode::serialize(&reply).expect("Serialization should not fail"),
        )
    })
}

pub fn register_server<
    Command: 'static + Clone + Serialize + DeserializeOwned + Default,
    R: 'static + AsRef<Raft<Command>> + Clone,
    S: AsRef<str>,
>(
    raft: R,
    name: S,
    network: &Mutex<Network>,
) -> std::io::Result<()> {
    let mut network = network.lock();
    let server_name = name.as_ref();
    let mut server = Server::make_server(server_name);

    let raft_clone = raft.clone();
    server.register_rpc_handler(
        REQUEST_VOTE_RPC.to_owned(),
        make_rpc_handler(move |args| {
            raft_clone.as_ref().process_request_vote(args)
        }),
    )?;

    let raft_clone = raft.clone();
    server.register_rpc_handler(
        APPEND_ENTRIES_RPC.to_owned(),
        make_rpc_handler(move |args| {
            raft_clone.as_ref().process_append_entries(args)
        }),
    )?;

    let raft_clone = raft;
    server.register_rpc_handler(
        INSTALL_SNAPSHOT_RPC.to_owned(),
        make_rpc_handler(move |args| {
            raft_clone.as_ref().process_install_snapshot(args)
        }),
    )?;

    network.add_server(server_name, server);

    Ok(())
}
pub fn register_kv_server<
    KV: 'static + AsRef<KVServer> + Clone,
    S: AsRef<str>,
>(
    kv: KV,
    name: S,
    network: &Mutex<Network>,
) -> std::io::Result<()> {
    let mut network = network.lock();
    let server_name = name.as_ref();
    let mut server = Server::make_server(server_name);

    let kv_clone = kv.clone();
    server.register_rpc_handler(
        GET.to_owned(),
        make_rpc_handler(move |args| kv_clone.as_ref().get(args)),
    )?;

    server.register_rpc_handler(
        PUT_APPEND.to_owned(),
        make_rpc_handler(move |args| kv.as_ref().put_append(args)),
    )?;

    network.add_server(server_name, server);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use ruaft::{ApplyCommandMessage, RemoteRaft, Term};

    use super::*;
    use ruaft::utils::integration_test::{
        make_append_entries_args, make_request_vote_args,
        unpack_append_entries_reply, unpack_request_vote_reply,
    };

    struct DoNothingPersister;
    impl ruaft::Persister for DoNothingPersister {
        fn read_state(&self) -> Bytes {
            Bytes::new()
        }

        fn save_state(&self, _bytes: Bytes) {}

        fn state_size(&self) -> usize {
            0
        }

        fn save_snapshot_and_state(&self, _: Bytes, _: &[u8]) {}
    }

    #[test]
    fn test_basic_message() -> std::io::Result<()> {
        test_utils::init_test_log!();

        let client = {
            let network = Network::run_daemon();
            let name = "test-basic-message";

            let client = network
                .lock()
                .make_client("test-basic-message", name.to_owned());

            let raft = Arc::new(Raft::new(
                vec![RpcClient(client)],
                0,
                Arc::new(DoNothingPersister),
                |_: ApplyCommandMessage<i32>| {},
                None,
                Raft::<i32>::NO_SNAPSHOT,
            ));
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

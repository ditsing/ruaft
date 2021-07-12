use async_trait::async_trait;
use labrpc::{Client, Network, ReplyMessage, RequestMessage, Server};
use parking_lot::Mutex;

use crate::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs,
    InstallSnapshotReply, Raft, RequestVoteArgs, RequestVoteReply,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub(crate) const REQUEST_VOTE_RPC: &str = "Raft.RequestVote";
pub(crate) const APPEND_ENTRIES_RPC: &str = "Raft.AppendEntries";
pub(crate) const INSTALL_SNAPSHOT_RPC: &str = "Raft.InstallSnapshot";

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

    pub(crate) async fn call_request_vote(
        &self,
        request: RequestVoteArgs,
    ) -> std::io::Result<RequestVoteReply> {
        self.call_rpc(REQUEST_VOTE_RPC, request).await
    }

    pub(crate) async fn call_append_entries<Command: Serialize>(
        &self,
        request: AppendEntriesArgs<Command>,
    ) -> std::io::Result<AppendEntriesReply> {
        self.call_rpc(APPEND_ENTRIES_RPC, request).await
    }

    pub(crate) async fn call_install_snapshot(
        &self,
        request: InstallSnapshotArgs,
    ) -> std::io::Result<InstallSnapshotReply> {
        self.call_rpc(INSTALL_SNAPSHOT_RPC, request).await
    }
}

#[async_trait]
impl<Command: 'static + Send + Serialize>
    crate::remote_raft::RemoteRaft<Command> for RpcClient
{
    async fn request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> std::io::Result<RequestVoteReply> {
        self.call_request_vote(args).await
    }

    async fn append_entries(
        &self,
        args: AppendEntriesArgs<Command>,
    ) -> std::io::Result<AppendEntriesReply> {
        self.call_append_entries(args).await
    }

    async fn install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> std::io::Result<InstallSnapshotReply> {
        self.call_install_snapshot(args).await
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use crate::{ApplyCommandMessage, Peer, Term};

    use super::*;

    type DoNothingPersister = ();
    impl crate::Persister for DoNothingPersister {
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
        let client = {
            let network = Network::run_daemon();
            let name = "test-basic-message";

            let client = network
                .lock()
                .make_client("test-basic-message", name.to_owned());

            let raft = Arc::new(Raft::new(
                vec![RpcClient(client)],
                0,
                Arc::new(()),
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
        let request = RequestVoteArgs {
            term: Term(2021),

            candidate_id: Peer(0),
            last_log_index: 0,
            last_log_term: Term(0),
        };
        let response =
            futures::executor::block_on(rpc_client.call_request_vote(request))?;
        assert!(response.vote_granted);

        let request = AppendEntriesArgs::<i32> {
            term: Term(2021),
            leader_id: Peer(0),
            prev_log_index: 0,
            prev_log_term: Term(0),
            entries: vec![],
            leader_commit: 0,
        };
        let response = futures::executor::block_on(
            rpc_client.call_append_entries(request),
        )?;
        assert_eq!(2021, response.term.0);
        assert!(response.success);

        Ok(())
    }
}

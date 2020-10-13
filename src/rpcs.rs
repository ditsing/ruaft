use std::sync::{Arc, Mutex};

use labrpc::{
    Client, Network, ReplyMessage, RequestMessage, RpcHandler, Server,
};

use crate::{
    AppendEntriesArgs, AppendEntriesReply, Raft, RequestVoteArgs,
    RequestVoteReply,
};

struct RequestVoteRpcHandler(Arc<Raft>);

impl RpcHandler for RequestVoteRpcHandler {
    fn call(&self, data: RequestMessage) -> ReplyMessage {
        let reply = self.0.process_request_vote(
            bincode::deserialize(data.as_ref())
                .expect("Deserialization of requests should not fail"),
        );

        ReplyMessage::from(
            bincode::serialize(&reply)
                .expect("Serialization of reply should not fail"),
        )
    }
}

struct AppendEntriesRpcHandler(Arc<Raft>);

impl RpcHandler for AppendEntriesRpcHandler {
    fn call(&self, data: RequestMessage) -> ReplyMessage {
        let reply = self.0.process_append_entries(
            bincode::deserialize(data.as_ref())
                .expect("Deserialization should not fail"),
        );

        ReplyMessage::from(
            bincode::serialize(&reply).expect("Serialization should not fail"),
        )
    }
}

pub(crate) const REQUEST_VOTE_RPC: &str = "Raft.RequestVote";
pub(crate) const APPEND_ENTRIES_RPC: &str = "Raft.AppendEntries";

#[derive(Clone)]
pub struct RpcClient(Client);

impl RpcClient {
    pub fn new(client: Client) -> Self {
        Self(client)
    }

    pub(crate) async fn call_request_vote(
        self: Self,
        request: RequestVoteArgs,
    ) -> std::io::Result<RequestVoteReply> {
        let data = RequestMessage::from(
            bincode::serialize(&request)
                .expect("Serialization of requests should not fail"),
        );

        let reply = self.0.call_rpc(REQUEST_VOTE_RPC.to_owned(), data).await?;

        Ok(bincode::deserialize(reply.as_ref())
            .expect("Deserialization of reply should not fail"))
    }

    pub(crate) async fn call_append_entries(
        self: Self,
        request: AppendEntriesArgs,
    ) -> std::io::Result<AppendEntriesReply> {
        let data = RequestMessage::from(
            bincode::serialize(&request)
                .expect("Serialization of requests should not fail"),
        );

        let reply =
            self.0.call_rpc(APPEND_ENTRIES_RPC.to_owned(), data).await?;

        Ok(bincode::deserialize(reply.as_ref())
            .expect("Deserialization of reply should not fail"))
    }
}

pub fn register_server<S: AsRef<str>>(
    raft: Arc<Raft>,
    name: S,
    network: &Mutex<Network>,
) -> std::io::Result<()> {
    let mut network =
        network.lock().expect("Network lock should not be poisoned");
    let server_name = name.as_ref().clone();
    let mut server = Server::make_server(server_name.clone());

    let request_vote_rpc_handler = RequestVoteRpcHandler(raft.clone());
    server.register_rpc_handler(
        REQUEST_VOTE_RPC.to_owned(),
        Box::new(request_vote_rpc_handler),
    )?;

    let append_entries_rpc_handler = AppendEntriesRpcHandler(raft);
    server.register_rpc_handler(
        APPEND_ENTRIES_RPC.to_owned(),
        Box::new(append_entries_rpc_handler),
    )?;

    network.add_server(server_name, server);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Peer, Term};

    #[test]
    fn test_basic_message() -> std::io::Result<()> {
        let client = {
            let network = Network::run_daemon();
            let name = "test-basic-message";

            let client = network
                .lock()
                .expect("Network lock should not be poisoned")
                .make_client("test-basic-message", name.to_owned());

            let raft = Arc::new(Raft::new(
                vec![RpcClient(client.clone())],
                0,
                |_, _| {},
            ));
            register_server(raft, name, network.as_ref())?;
            client
        };

        let rpc_client = RpcClient(client);
        let request = RequestVoteArgs {
            term: Term(2021),

            candidate_id: Peer(0),
            last_log_index: 0,
            last_log_term: Term(0),
        };
        let response = futures::executor::block_on(
            rpc_client.clone().call_request_vote(request),
        )?;
        assert_eq!(true, response.vote_granted);

        let request = AppendEntriesArgs {
            term: Term(2021),
            leader_id: Peer(0),
            prev_log_index: 0,
            prev_log_term: Term(0),
            entries: vec![],
            leader_commit: 0,
        };
        let response = futures::executor::block_on(
            rpc_client.clone().call_append_entries(request),
        )?;
        assert_eq!(2021, response.term.0);
        assert_eq!(true, response.success);

        Ok(())
    }
}

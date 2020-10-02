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

pub(crate) const REQUEST_VOTE_RPC: &'static str = "Raft.RequestVote";
pub(crate) async fn call_request_vote(
    client: &Client,
    request: RequestVoteArgs,
) -> std::io::Result<RequestVoteReply> {
    let data = RequestMessage::from(
        bincode::serialize(&request)
            .expect("Serialization of requests should not fail"),
    );
    let reply = client.call_rpc(REQUEST_VOTE_RPC.to_owned(), data).await?;

    Ok(bincode::deserialize(reply.as_ref())
        .expect("Deserialization of reply should not fail"))
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

pub(crate) const APPEND_ENTRIES_RPC: &'static str = "Raft.AppendEntries";
pub(crate) async fn call_append_entries(
    client: &Client,
    request: AppendEntriesArgs,
) -> std::io::Result<AppendEntriesReply> {
    let data = RequestMessage::from(
        bincode::serialize(&request)
            .expect("Serialization of requests should not fail"),
    );
    let reply = client.call_rpc(APPEND_ENTRIES_RPC.to_owned(), data).await?;

    Ok(bincode::deserialize(reply.as_ref())
        .expect("Deserialization of reply should not fail"))
}

pub(crate) fn register_server<S: AsRef<str>>(
    raft: Arc<Raft>,
    name: S,
    network: Arc<Mutex<Network>>,
) -> std::io::Result<()> {
    let mut network =
        network.lock().expect("Network lock should not be poisoned");
    let server_name = format!("{}-server", name.as_ref());
    let mut server = Server::make_server(server_name.clone());

    let request_vote_rpc_handler = RequestVoteRpcHandler(raft.clone());
    server.register_rpc_handler(
        REQUEST_VOTE_RPC.to_owned(),
        Box::new(request_vote_rpc_handler),
    )?;

    let append_entries_rpc_handler = AppendEntriesRpcHandler(raft.clone());
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

    fn test_basic_message() -> std::io::Result<()> {
        let network = Network::run_daemon();
        let raft = Arc::new(Raft {});
        let name = "test-basic-message";

        register_server(raft, name, network.clone())?;
        let client = network
            .lock()
            .expect("Network lock should not be poisoned")
            .make_client("test-basic-message", name);

        let request = RequestVoteArgs { term: 2021 };
        let response =
            futures::executor::block_on(call_request_vote(&client, request))?;
        assert_eq!(2022, response.term);

        let request = AppendEntriesArgs { term: 2021 };
        let response =
            futures::executor::block_on(call_append_entries(&client, request))?;
        assert_eq!(2020, response.term);

        Ok(())
    }
}

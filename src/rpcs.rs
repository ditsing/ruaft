use std::sync::Arc;

use labrpc::{Client, Network, ReplyMessage, RequestMessage, Server};
use parking_lot::Mutex;

use crate::{
    AppendEntriesArgs, AppendEntriesReply, Raft, RequestVoteArgs,
    RequestVoteReply,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

fn proxy_request_vote<Command: Clone + Serialize + Default>(
    raft: &Raft<Command>,
    data: RequestMessage,
) -> ReplyMessage {
    let reply = raft.process_request_vote(
        bincode::deserialize(data.as_ref())
            .expect("Deserialization of requests should not fail"),
    );

    ReplyMessage::from(
        bincode::serialize(&reply)
            .expect("Serialization of reply should not fail"),
    )
}

fn proxy_append_entries<
    Command: Clone + Serialize + DeserializeOwned + Default,
>(
    raft: &Raft<Command>,
    data: RequestMessage,
) -> ReplyMessage {
    let reply = raft.process_append_entries(
        bincode::deserialize(data.as_ref())
            .expect("Deserialization should not fail"),
    );

    ReplyMessage::from(
        bincode::serialize(&reply).expect("Serialization should not fail"),
    )
}

pub(crate) const REQUEST_VOTE_RPC: &str = "Raft.RequestVote";
pub(crate) const APPEND_ENTRIES_RPC: &str = "Raft.AppendEntries";

pub struct RpcClient(Client);

impl RpcClient {
    pub fn new(client: Client) -> Self {
        Self(client)
    }

    pub(crate) async fn call_request_vote(
        &self,
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

    pub(crate) async fn call_append_entries<Command: Serialize>(
        &self,
        request: AppendEntriesArgs<Command>,
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

pub fn register_server<
    Command: 'static + Clone + Serialize + DeserializeOwned + Default,
    S: AsRef<str>,
>(
    raft: Arc<Raft<Command>>,
    name: S,
    network: &Mutex<Network>,
) -> std::io::Result<()> {
    let mut network = network.lock();
    let server_name = name.as_ref();
    let mut server = Server::make_server(server_name);

    let raft_clone = raft.clone();
    server.register_rpc_handler(
        REQUEST_VOTE_RPC.to_owned(),
        Box::new(move |request| {
            proxy_request_vote(raft_clone.as_ref(), request)
        }),
    )?;

    let raft_clone = raft;
    server.register_rpc_handler(
        APPEND_ENTRIES_RPC.to_owned(),
        Box::new(move |request| {
            proxy_append_entries(raft_clone.as_ref(), request)
        }),
    )?;

    network.add_server(server_name, server);

    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{Peer, Term};

    use super::*;

    type DoNothingPersister = ();
    impl crate::Persister for DoNothingPersister {
        fn read_state(&self) -> Bytes {
            Bytes::new()
        }

        fn save_state(&self, _bytes: Bytes) {}
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
                |_, _: i32| {},
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
        assert_eq!(true, response.vote_granted);

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
        assert_eq!(true, response.success);

        Ok(())
    }
}

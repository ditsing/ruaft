#![allow(unused)]

extern crate bincode;
extern crate labrpc;
#[macro_use]
extern crate serde_derive;
extern crate tokio;

mod rpcs;

struct Raft {}

#[derive(Serialize, Deserialize)]
struct RequestVoteArgs {
    term: i64,
}

#[derive(Serialize, Deserialize)]
struct RequestVoteReply {
    term: i64,
}

#[derive(Serialize, Deserialize)]
struct AppendEntriesArgs {
    term: i64,
}

#[derive(Serialize, Deserialize)]
struct AppendEntriesReply {
    term: i64,
}

impl Raft {
    pub(crate) fn process_request_vote(
        &self,
        request: RequestVoteArgs,
    ) -> RequestVoteReply {
        RequestVoteReply {
            term: request.term + 1,
        }
    }

    pub(crate) fn process_append_entries(
        &self,
        request: AppendEntriesArgs,
    ) -> AppendEntriesReply {
        AppendEntriesReply {
            term: request.term - 1,
        }
    }
}

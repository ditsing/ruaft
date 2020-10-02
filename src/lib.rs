#![allow(unused)]

extern crate bincode;
extern crate labrpc;
#[macro_use]
extern crate serde_derive;
extern crate tokio;

use crate::rpcs::RpcClient;
use parking_lot::{Condvar, Mutex};
use std::sync::atomic::AtomicBool;

pub mod rpcs;

enum State {
    Follower,
    Candidate,
    // TODO: add PreVote
    Leader,
}

// TODO: remove all of the defaults.
impl Default for State {
    fn default() -> Self {
        Self::Leader
    }
}

#[derive(Default)]
struct RaftState {
    current_term: usize,
    voted_for: i64,
    // TODO: Allow sending of arbitrary information.
    log: Vec<usize>,

    commit_index: usize,
    last_applied: usize,

    next_index: Vec<usize>,
    match_index: Vec<usize>,
    current_step: Vec<i64>,

    state: State,

    leader_id: usize,
    // election_timer: timer,
}

#[derive(Default)]
struct Raft {
    inner_state: Mutex<RaftState>,
    peers: RpcClient,

    me: usize,

    vote_mutex: Mutex<()>,
    vote_cond: Condvar,

    // new_log_entry: Sender<usize>,
    // new_log_entry: Receiver<usize>,
    // apply_command_cond: Condvar

    keep_running: AtomicBool,

    // applyCh: Sender<ApplyMsg>
}

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

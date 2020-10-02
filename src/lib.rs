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

struct LogEntry {
    term: usize,
    index: usize,
    // TODO: Allow sending of arbitrary information.
    command: usize,
}

#[derive(Default)]
struct RaftState {
    current_term: usize,
    voted_for: Option<usize>,
    log: Vec<LogEntry>,

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
    term: usize,
    candidate_id: usize,
    last_log_index: usize,
    last_log_term: usize,
}

#[derive(Serialize, Deserialize)]
struct RequestVoteReply {
    term: usize,
    vote_granted: bool,
}

#[derive(Serialize, Deserialize)]
struct AppendEntriesArgs {
    term: usize,
}

#[derive(Serialize, Deserialize)]
struct AppendEntriesReply {
    term: usize,
}

impl Raft {
    pub(crate) fn process_request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> RequestVoteReply {
        let mut rf = self.inner_state.lock();

        let term = rf.current_term;
        if args.term < term {
            return RequestVoteReply {
                term,
                vote_granted: false,
            };
        } else if args.term > term {
            rf.current_term = args.term;
            rf.voted_for = None;
            rf.state = State::Follower;
            // TODO: quit current election
            // TODO: reset election timer
            // TODO: persist
        }

        let voted_for = rf.voted_for;
        let last_log_index = rf.log.len() - 1;
        let last_log_term = rf.log.last().unwrap().term;
        if (voted_for.is_none() || voted_for == Some(args.candidate_id))
            && (args.last_log_term > last_log_term
                || (args.last_log_term == last_log_term
                    && args.last_log_index >= last_log_index))
        {
            rf.voted_for = Some(args.candidate_id);
            // TODO: reset election timer.
            // TODO: persist

            RequestVoteReply {
                term: args.term,
                vote_granted: true,
            }
        } else {
            RequestVoteReply {
                term: args.term,
                vote_granted: false,
            }
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

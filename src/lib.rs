#![allow(unused)]

extern crate bincode;
extern crate labrpc;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate tokio;

use crate::rpcs::RpcClient;
use parking_lot::{Condvar, Mutex};
use rand::{thread_rng, Rng};
use std::sync::atomic::AtomicBool;
use tokio::time::Duration;

pub mod rpcs;

enum State {
    Follower,
    Candidate,
    // TODO: add PreVote
    Leader,
}

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize,
)]
struct Term(usize);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Peer(usize);

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct Command(usize);

// TODO: remove all of the defaults.
impl Default for State {
    fn default() -> Self {
        Self::Leader
    }
}
impl Default for Term {
    fn default() -> Self {
        Self(0)
    }
}
impl Default for Peer {
    fn default() -> Self {
        Self(0)
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct LogEntry {
    term: Term,
    index: usize,
    // TODO: Allow sending of arbitrary information.
    command: Command,
}

#[derive(Default)]
struct RaftState {
    current_term: Term,
    voted_for: Option<Peer>,
    log: Vec<LogEntry>,

    commit_index: usize,
    last_applied: usize,

    next_index: Vec<usize>,
    match_index: Vec<usize>,
    current_step: Vec<i64>,

    state: State,

    leader_id: Peer,
    // Timer will be removed upon shutdown.
    election_timer: Option<tokio::time::Delay>,
}

#[derive(Default)]
struct Raft {
    inner_state: Mutex<RaftState>,
    peers: RpcClient,

    me: Peer,

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
    term: Term,
    candidate_id: Peer,
    last_log_index: usize,
    last_log_term: Term,
}

#[derive(Serialize, Deserialize)]
struct RequestVoteReply {
    term: Term,
    vote_granted: bool,
}

#[derive(Serialize, Deserialize)]
struct AppendEntriesArgs {
    term: Term,
    leader_id: Peer,
    prev_log_index: usize,
    prev_log_term: Term,
    entries: Vec<LogEntry>,
    leader_commit: usize,
}

#[derive(Serialize, Deserialize)]
struct AppendEntriesReply {
    term: Term,
    success: bool,
}

impl Raft {
    pub fn new() -> Self {
        let mut raft = Self {
            ..Default::default()
        };
        raft.inner_state.lock().log.push(LogEntry {
            term: Default::default(),
            index: 0,
            command: Command(0),
        });
        raft
    }

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
            rf.reset_election_timer();
            rf.persist();
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
            rf.reset_election_timer();
            rf.persist();

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
        args: AppendEntriesArgs,
    ) -> AppendEntriesReply {
        let mut rf = self.inner_state.lock();
        if rf.current_term > args.term {
            return AppendEntriesReply {
                term: rf.current_term,
                success: false,
            };
        }

        if rf.current_term < args.term {
            rf.current_term = args.term;
            rf.voted_for = None;
        }

        rf.state = State::Follower;
        rf.reset_election_timer();
        // TODO: stop previous election
        rf.leader_id = args.leader_id;

        if rf.log.len() <= args.prev_log_index
            || rf.log[args.prev_log_index].term != args.term
        {
            return AppendEntriesReply {
                term: args.term,
                success: false,
            };
        }

        for (i, entry) in args.entries.iter().enumerate() {
            let index = i + args.prev_log_index + 1;
            if rf.log.len() > index {
                if rf.log[index].term != entry.term {
                    rf.log.truncate(index);
                    rf.log.push(entry.clone());
                }
            } else {
                rf.log.push(entry.clone());
            }
        }

        if args.leader_commit > rf.commit_index {
            rf.commit_index = if args.leader_commit < rf.log.len() {
                args.leader_commit
            } else {
                rf.log.len() - 1
            };
            // TODO: apply commands.
        }

        AppendEntriesReply {
            term: args.term,
            success: true,
        }
    }
}

const HEARTBEAT_INTERVAL_MILLIS: u64 = 150;
const ELECTION_TIMEOUT_BASE_MILLIS: u64 = 150;
const ELECTION_TIMEOUT_VAR_MILLIS: u64 = 250;

impl RaftState {
    fn reset_election_timer(&mut self) {
        self.election_timer.as_mut().map(|timer| {
            timer.reset(tokio::time::Instant::now() + Self::election_timeout())
        });
    }

    fn election_timeout() -> Duration {
        Duration::from_millis(
            ELECTION_TIMEOUT_BASE_MILLIS
                + thread_rng().gen_range(0, ELECTION_TIMEOUT_VAR_MILLIS),
        )
    }

    fn persist(&self) {
        // TODO: implement
    }
}

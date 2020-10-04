#![allow(unused)]

extern crate bincode;
extern crate futures;
extern crate labrpc;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate tokio;

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use parking_lot::{Condvar, Mutex};
use rand::{thread_rng, Rng};

use crate::rpcs::RpcClient;
use std::cell::RefCell;

pub mod rpcs;

#[derive(Eq, PartialEq)]
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

    // Current election cancel token, might be None if no election is running.
    election_cancel_token: Option<futures::channel::oneshot::Sender<Term>>,
    // Timer will be removed upon shutdown or elected.
    election_timer: Option<tokio::time::Delay>,
}

#[derive(Default)]
struct Raft {
    inner_state: Arc<Mutex<RaftState>>,
    peers: Vec<RpcClient>,

    me: Peer,

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
            rf.reset_election_timer();
            rf.stop_current_election();
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
            // No need to stop the election. We are not a candidate.
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
        rf.stop_current_election();
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

    async fn retry_rpc<Func, Fut, T>(
        max_retry: usize,
        mut task_gen: Func,
    ) -> std::io::Result<T>
    where
        Fut: Future<Output = std::io::Result<T>> + Send + 'static,
        Func: FnMut(usize) -> Fut,
    {
        for i in 0..max_retry {
            if let Ok(reply) = task_gen(i).await {
                return Ok(reply);
            }
            tokio::time::delay_for(Duration::from_millis((1 << i) * 10)).await;
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("Timed out after {} retries", max_retry),
        ))
    }

    fn run_election(&self) {
        let (term, last_log_index, last_log_term, cancel_token) = {
            let mut rf = self.inner_state.lock();

            let (tx, rx) = futures::channel::oneshot::channel();
            rf.current_term.0 += 1;

            rf.voted_for = Some(self.me);
            rf.state = State::Candidate;
            rf.reset_election_timer();
            rf.stop_current_election();

            rf.election_cancel_token.replace(tx);

            rf.persist();

            (
                rf.current_term,
                rf.log.len() - 1,
                rf.log.last().unwrap().term,
                rx,
            )
        };

        let me = self.me;

        let mut votes = vec![];
        for i in 0..self.peers.len() {
            if i != self.me.0 {
                // Make a clone now so that self will not be passed across await
                // boundary.
                let rpc_client = self.peers[i].clone();
                // RPCs are started right away.
                let one_vote = tokio::spawn(async move {
                    let reply_future = Self::retry_rpc(4, move |_round| {
                        rpc_client.clone().call_request_vote(RequestVoteArgs {
                            term,
                            candidate_id: me,
                            last_log_index,
                            last_log_term,
                        })
                    });
                    if let Ok(reply) = reply_future.await {
                        return Some(reply.vote_granted && reply.term == term);
                    }
                    return None;
                });
                // Futures must be pinned so that they have Unpin, as required
                // by futures::future::select.
                votes.push(one_vote);
            }
        }

        tokio::spawn(Self::count_vote_util_cancelled(
            term,
            self.inner_state.clone(),
            votes,
            self.peers.len() / 2,
            cancel_token,
        ));
    }

    async fn count_vote_util_cancelled(
        term: Term,
        rf: Arc<Mutex<RaftState>>,
        votes: Vec<tokio::task::JoinHandle<Option<bool>>>,
        majority: usize,
        cancel_token: futures::channel::oneshot::Receiver<Term>,
    ) {
        let mut vote_count = 0;
        let mut against_count = 0;
        let mut cancel_token = cancel_token;
        let mut futures_vec = votes;
        while vote_count < majority && against_count <= majority {
            // Mixing tokio futures with futures-rs ones. Fingers crossed.
            let selected = futures::future::select(
                cancel_token,
                futures::future::select_all(futures_vec),
            )
            .await;
            let ((one_vote, index, rest), new_token) = match selected {
                futures::future::Either::Left(_) => break,
                futures::future::Either::Right(tuple) => tuple,
            };

            futures_vec = rest;
            cancel_token = new_token;

            if let Ok(Some(vote)) = one_vote {
                if vote {
                    vote_count += 1
                } else {
                    against_count += 1
                }
            }
        }

        if vote_count < majority {
            return;
        }
        let mut rf = rf.lock();
        if rf.current_term == term && rf.state == State::Candidate {
            rf.state = State::Leader;
        }
        let log_len = rf.log.len();
        for item in rf.next_index.iter_mut() {
            *item = log_len;
        }
        for item in rf.match_index.iter_mut() {
            *item = 0;
        }
        // TODO: send heartbeats.
        // Drop the timer and cancel token.
        rf.election_cancel_token.take();
        rf.election_timer.take();
        rf.persist();
    }
}

const HEARTBEAT_INTERVAL_MILLIS: u64 = 150;
const ELECTION_TIMEOUT_BASE_MILLIS: u64 = 150;
const ELECTION_TIMEOUT_VAR_MILLIS: u64 = 250;

impl RaftState {
    fn reset_election_timer(&mut self) {
        self.election_timer.as_mut().map(|timer| {
            timer.reset(
                (std::time::Instant::now() + Self::election_timeout()).into(),
            )
        });
    }

    fn election_timeout() -> Duration {
        Duration::from_millis(
            ELECTION_TIMEOUT_BASE_MILLIS
                + thread_rng().gen_range(0, ELECTION_TIMEOUT_VAR_MILLIS),
        )
    }

    fn stop_current_election(&mut self) {
        self.election_cancel_token
            .take()
            .map(|sender| sender.send(self.current_term));
    }

    fn persist(&self) {
        // TODO: implement
    }
}

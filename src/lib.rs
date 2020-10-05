#![allow(unused)]

extern crate bincode;
extern crate futures;
extern crate labrpc;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate tokio;

use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rand::{thread_rng, Rng};

use crate::rpcs::RpcClient;
use crate::utils::retry_rpc;

pub mod rpcs;
mod utils;

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

#[derive(Clone, Serialize, Deserialize)]
struct RequestVoteArgs {
    term: Term,
    candidate_id: Peer,
    last_log_index: usize,
    last_log_term: Term,
}

#[derive(Clone, Serialize, Deserialize)]
struct RequestVoteReply {
    term: Term,
    vote_granted: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct AppendEntriesArgs {
    term: Term,
    leader_id: Peer,
    prev_log_index: usize,
    prev_log_term: Term,
    entries: Vec<LogEntry>,
    leader_commit: usize,
}

#[derive(Clone, Serialize, Deserialize)]
struct AppendEntriesReply {
    term: Term,
    success: bool,
}

impl Raft {
    pub fn new() -> Self {
        let raft = Self {
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
        let (last_log_index, last_log_term) = rf.last_log_index_and_term();
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
        for (index, rpc_client) in self.peers.iter().enumerate() {
            if index != self.me.0 {
                // RpcClient must be cloned to avoid sending its reference
                // across threads.
                let rpc_client = rpc_client.clone();
                // RPCs are started right away.
                let one_vote = tokio::spawn(Self::request_one_vote(
                    rpc_client,
                    term,
                    me,
                    last_log_index,
                    last_log_term,
                ));
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

    async fn request_one_vote(
        rpc_client: RpcClient,
        term: Term,
        me: Peer,
        last_log_index: usize,
        last_log_term: Term,
    ) -> Option<bool> {
        let reply = retry_rpc(4, move |_round| {
            rpc_client.clone().call_request_vote(RequestVoteArgs {
                term,
                candidate_id: me,
                last_log_index,
                last_log_term,
            })
        })
        .await;
        if let Ok(reply) = reply {
            return Some(reply.vote_granted && reply.term == term);
        }
        return None;
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
            let ((one_vote, _, rest), new_token) = match selected {
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

    fn schedule_heartbeats(&self, interval: Duration) {
        for (peer_index, rpc_client) in self.peers.iter().enumerate() {
            if peer_index != self.me.0 {
                // Interval and rf are now owned by the outer async function.
                let mut interval = tokio::time::interval(interval);
                let rf = self.inner_state.clone();
                // RPC client must be cloned into the outer async function.
                let rpc_client = rpc_client.clone();
                tokio::spawn(async move {
                    loop {
                        // TODO: shutdown signal or cancel token.
                        interval.tick().await;
                        tokio::spawn(Self::send_heartbeat(
                            rf.clone(),
                            rpc_client.clone(),
                        ));
                    }
                });
            }
        }
    }

    const HEARTBEAT_RETRY: usize = 3;
    async fn send_heartbeat(
        rf: Arc<Mutex<RaftState>>,
        rpc_client: RpcClient,
    ) -> std::io::Result<()> {
        let (is_leader, args) = {
            // Making sure locked rf is out of scope for the following await
            let rf = rf.lock();
            // copy states.
            let term = rf.current_term;
            let is_leader = rf.state == State::Leader;
            let (last_log_index, last_log_term) = rf.last_log_index_and_term();
            let commit_index = rf.commit_index;
            let leader_id = rf.leader_id;

            let args = AppendEntriesArgs {
                term,
                leader_id,
                prev_log_index: last_log_index,
                prev_log_term: last_log_term,
                entries: vec![],
                leader_commit: commit_index,
            };
            (is_leader, args)
        };

        if is_leader {
            retry_rpc(Self::HEARTBEAT_RETRY, move |_round| {
                rpc_client.clone().call_append_entries(args.clone())
            })
            .await?;
        }
        Ok(())
    }

    const APPEND_ENTRIES_RETRY: usize = 3;
    fn run_log_entry_daemon(
        &self,
    ) -> (
        std::thread::JoinHandle<()>,
        std::sync::mpsc::Sender<Option<Peer>>,
    ) {
        let (tx, rx) = std::sync::mpsc::channel::<Option<Peer>>();

        // Clone everything that the thread needs.
        let rerun = tx.clone();
        let peers = self.peers.clone();
        let rf = self.inner_state.clone();
        let me = self.me;
        let handle = std::thread::spawn(move || {
            while let Ok(peer) = rx.recv() {
                for (i, rpc_client) in peers.iter().enumerate() {
                    if i != me.0 && peer.map(|p| p.0 == i).unwrap_or(true) {
                        let rf = rf.clone();
                        let rpc_client = rpc_client.clone();
                        let rerun = rerun.clone();
                        let peer_index = i;
                        tokio::spawn(async move {
                            // TODO: cancel in flight changes?
                            let rf_clone = rf.clone();
                            let succeeded = retry_rpc(
                                Self::APPEND_ENTRIES_RETRY,
                                move |_round| {
                                    Self::append_entries(
                                        rf.clone(),
                                        rpc_client.clone(),
                                        peer_index,
                                    )
                                },
                            )
                            .await;
                            match succeeded {
                                Ok(done) => {
                                    if !done {
                                        let mut rf = rf_clone.lock();

                                        let step =
                                            &mut rf.current_step[peer_index];
                                        *step += 1;
                                        let diff = (1 << 8) << *step;

                                        let next_index =
                                            &mut rf.next_index[peer_index];
                                        if diff >= *next_index {
                                            *next_index = 1usize;
                                        } else {
                                            *next_index -= diff;
                                        }

                                        rerun.send(Some(Peer(peer_index)));
                                    }
                                }
                                Err(_) => {
                                    tokio::time::delay_for(
                                        Duration::from_millis(
                                            HEARTBEAT_INTERVAL_MILLIS,
                                        ),
                                    )
                                    .await;
                                    rerun.send(Some(Peer(peer_index)));
                                }
                            };
                        });
                    }
                }
            }
        });

        (handle, tx)
    }

    async fn append_entries(
        rf: Arc<Mutex<RaftState>>,
        rpc_client: RpcClient,
        peer_index: usize,
    ) -> std::io::Result<bool> {
        let (term, result) = {
            let rf = rf.lock();
            let term = rf.current_term;
            let (prev_log_index, prev_log_term) = rf.last_log_index_and_term();
            let result = rpc_client.call_append_entries(AppendEntriesArgs {
                term: rf.current_term,
                leader_id: rf.leader_id,
                prev_log_index,
                prev_log_term,
                entries: rf.log[rf.next_index[peer_index]..].to_vec(),
                leader_commit: rf.commit_index,
            });
            (term, result)
        };
        let reply = result.await?;
        let ret = reply.term != term || reply.success;
        Ok(ret)
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

    fn last_log_index_and_term(&self) -> (usize, Term) {
        let len = self.log.len();
        assert!(len > 0, "There should always be at least one entry in log");
        (len - 1, self.log.last().unwrap().term)
    }
}

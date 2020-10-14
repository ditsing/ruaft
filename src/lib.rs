extern crate bincode;
extern crate futures;
extern crate labrpc;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate tokio;

use std::convert::TryFrom;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_utils::sync::WaitGroup;
use parking_lot::{Condvar, Mutex};
use rand::{thread_rng, Rng};

use crate::persister::PersistedRaftState;
pub use crate::persister::Persister;
pub use crate::rpcs::RpcClient;
use crate::utils::{retry_rpc, DropGuard};

mod persister;
pub mod rpcs;
pub mod utils;

#[derive(Debug, Eq, PartialEq)]
enum State {
    Follower,
    Candidate,
    // TODO: add PreVote
    Leader,
}

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Term(pub usize);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Peer(usize);

pub type Index = usize;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Command(pub i32);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LogEntry {
    term: Term,
    index: Index,
    // TODO: Allow sending of arbitrary information.
    command: Command,
}

struct RaftState {
    current_term: Term,
    voted_for: Option<Peer>,
    log: Vec<LogEntry>,

    commit_index: Index,
    last_applied: Index,

    next_index: Vec<Index>,
    match_index: Vec<Index>,
    current_step: Vec<i64>,

    state: State,

    leader_id: Peer,
}

struct ElectionState {
    // Timer will be removed upon shutdown or elected.
    timer: Mutex<(usize, Option<Instant>)>,
    // Wake up the timer thread when the timer is reset or cancelled.
    signal: Condvar,
}

#[derive(Clone)]
pub struct Raft {
    inner_state: Arc<Mutex<RaftState>>,
    peers: Vec<RpcClient>,

    me: Peer,

    persister: Arc<dyn Persister>,

    new_log_entry: Option<std::sync::mpsc::Sender<Option<Peer>>>,
    apply_command_signal: Arc<Condvar>,
    keep_running: Arc<AtomicBool>,
    election: Arc<ElectionState>,

    thread_pool: Arc<tokio::runtime::Runtime>,

    stop_wait_group: WaitGroup,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RequestVoteArgs {
    term: Term,
    candidate_id: Peer,
    last_log_index: Index,
    last_log_term: Term,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RequestVoteReply {
    term: Term,
    vote_granted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AppendEntriesArgs {
    term: Term,
    leader_id: Peer,
    prev_log_index: Index,
    prev_log_term: Term,
    entries: Vec<LogEntry>,
    leader_commit: Index,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AppendEntriesReply {
    term: Term,
    success: bool,
}

impl Raft {
    pub fn new<Func>(
        peers: Vec<RpcClient>,
        me: usize,
        persister: Arc<dyn Persister>,
        apply_command: Func,
    ) -> Self
    where
        Func: 'static + Send + FnMut(Index, Command),
    {
        let peer_size = peers.len();
        let mut state = RaftState {
            current_term: Term(0),
            voted_for: None,
            log: vec![LogEntry {
                term: Term(0),
                index: 0,
                command: Command(0),
            }],
            commit_index: 0,
            last_applied: 0,
            next_index: vec![1; peer_size],
            match_index: vec![0; peer_size],
            current_step: vec![0; peer_size],
            state: State::Follower,
            leader_id: Peer(me),
        };

        if let Ok(persisted_state) =
            PersistedRaftState::try_from(persister.read_state())
        {
            state.current_term = persisted_state.current_term;
            state.voted_for = persisted_state.voted_for;
            state.log = persisted_state.log;
        }

        let election = ElectionState {
            timer: Mutex::new((0, None)),
            signal: Condvar::new(),
        };
        election.reset_election_timer();

        let thread_pool = tokio::runtime::Builder::new()
            .threaded_scheduler()
            .enable_time()
            .thread_name(format!("raft-instance-{}", me))
            .core_threads(peer_size)
            .max_threads(peer_size * 2)
            .build()
            .expect("Creating thread pool should not fail");
        let mut this = Raft {
            inner_state: Arc::new(Mutex::new(state)),
            peers,
            me: Peer(me),
            persister,
            new_log_entry: None,
            apply_command_signal: Arc::new(Default::default()),
            keep_running: Arc::new(Default::default()),
            election: Arc::new(election),
            thread_pool: Arc::new(thread_pool),
            stop_wait_group: WaitGroup::new(),
        };

        this.keep_running.store(true, Ordering::SeqCst);
        // Running in a standalone thread.
        this.run_log_entry_daemon();
        // Running in a standalone thread.
        this.run_apply_command_daemon(apply_command);
        // One off function that schedules many little tasks, running on the
        // internal thread pool.
        this.schedule_heartbeats(Duration::from_millis(
            HEARTBEAT_INTERVAL_MILLIS,
        ));
        // The last step is to start running election timer.
        this.run_election_timer();
        this
    }

    pub(crate) fn process_request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> RequestVoteReply {
        let mut rf = self.inner_state.lock();

        let term = rf.current_term;
        #[allow(clippy::comparison_chain)]
        if args.term < term {
            return RequestVoteReply {
                term,
                vote_granted: false,
            };
        } else if args.term > term {
            rf.current_term = args.term;
            rf.voted_for = None;
            rf.state = State::Follower;

            self.election.reset_election_timer();
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

            // It is possible that we have set a timer above when updating the
            // current term. It does not hurt to update the timer again.
            // We do need to persist, though.
            self.election.reset_election_timer();
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

        let _ = rf.deferred_persist();

        if rf.current_term < args.term {
            rf.current_term = args.term;
            rf.voted_for = None;
        }

        rf.state = State::Follower;
        rf.leader_id = args.leader_id;

        self.election.reset_election_timer();

        if rf.log.len() <= args.prev_log_index
            || rf.log[args.prev_log_index].term != args.prev_log_term
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
            self.apply_command_signal.notify_one();
        }

        AppendEntriesReply {
            term: args.term,
            success: true,
        }
    }

    fn run_election_timer(&self) -> std::thread::JoinHandle<()> {
        let this = self.clone();
        std::thread::spawn(move || {
            let election = this.election.clone();

            let mut should_run = None;
            while this.keep_running.load(Ordering::SeqCst) {
                let mut cancel_handle = should_run
                    .map(|last_timer_count| this.run_election(last_timer_count))
                    .flatten();

                let mut guard = election.timer.lock();
                let (timer_count, deadline) = *guard;
                if let Some(last_timer_count) = should_run {
                    // If the timer was changed more than once, we know the
                    // last scheduled election should have been cancelled.
                    if timer_count > last_timer_count + 1 {
                        cancel_handle.take().map(|c| c.send(()));
                    }
                }
                should_run = match deadline {
                    Some(timeout) => loop {
                        let ret =
                            election.signal.wait_until(&mut guard, timeout);
                        let fired = ret.timed_out() && Instant::now() > timeout;
                        // If the timer has been updated, do not schedule,
                        // break so that we could cancel.
                        if timer_count != guard.0 {
                            // Timer has been updated, cancel current
                            // election, and block on timeout again.
                            break None;
                        } else if fired {
                            // Timer has fired, remove the timer and allow
                            // running the next election at timer_count.
                            // If the next election is cancelled before we
                            // are back on wait, timer_count will be set to
                            // a different value.
                            guard.0 += 1;
                            guard.1.take();
                            break Some(guard.0);
                        }
                    },
                    None => {
                        election.signal.wait(&mut guard);
                        // The timeout has changed, check again.
                        None
                    }
                };
                drop(guard);
                // Whenever woken up, cancel the current running election.
                // There are 3 cases we could reach here
                // 1. We received an AppendEntries, or decided to vote for
                // a peer, and thus turned into a follower. In this case we'll
                // be notified by the election signal.
                // 2. We are a follower but didn't receive a heartbeat on time,
                // or we are a candidate but didn't not collect enough vote on
                // time. In this case we'll have a timeout.
                // 3. When become a leader, or are shutdown. In this case we'll
                // be notified by the election signal.
                cancel_handle.map(|c| c.send(()));
            }

            let stop_wait_group = this.stop_wait_group.clone();
            // Making sure the rest of `this` is dropped before the wait group.
            drop(this);
            drop(stop_wait_group);
        })
    }

    fn run_election(
        &self,
        timer_count: usize,
    ) -> Option<futures::channel::oneshot::Sender<()>> {
        let me = self.me;
        let (term, args) = {
            let mut rf = self.inner_state.lock();

            // The previous election is faster and reached the critical section
            // before us. We should stop and not run this election.
            // Or someone else increased the term and the timer is reset.
            if !self.election.try_reset_election_timer(timer_count) {
                return None;
            }

            rf.current_term.0 += 1;

            rf.voted_for = Some(me);
            rf.state = State::Candidate;

            rf.persist();

            let term = rf.current_term;
            let (last_log_index, last_log_term) = rf.last_log_index_and_term();

            (
                term,
                RequestVoteArgs {
                    term,
                    candidate_id: me,
                    last_log_index,
                    last_log_term,
                },
            )
        };

        let mut votes = vec![];
        for (index, rpc_client) in self.peers.iter().enumerate() {
            if index != self.me.0 {
                // RpcClient must be cloned to avoid sending its reference
                // across threads.
                let rpc_client = rpc_client.clone();
                // RPCs are started right away.
                let one_vote = self
                    .thread_pool
                    .spawn(Self::request_vote(rpc_client, args.clone()));
                // Futures must be pinned so that they have Unpin, as required
                // by futures::future::select.
                votes.push(one_vote);
            }
        }

        let (tx, rx) = futures::channel::oneshot::channel();
        self.thread_pool.spawn(Self::count_vote_util_cancelled(
            term,
            self.inner_state.clone(),
            self.election.clone(),
            votes,
            self.peers.len() / 2,
            rx,
            self.new_log_entry.clone().unwrap(),
        ));
        Some(tx)
    }

    const REQUEST_VOTE_RETRY: usize = 4;
    async fn request_vote(
        rpc_client: RpcClient,
        args: RequestVoteArgs,
    ) -> Option<bool> {
        let term = args.term;
        let reply = retry_rpc(Self::REQUEST_VOTE_RETRY, move |_round| {
            rpc_client.clone().call_request_vote(args.clone())
        })
        .await;
        if let Ok(reply) = reply {
            return Some(reply.vote_granted && reply.term == term);
        }
        None
    }

    async fn count_vote_util_cancelled(
        term: Term,
        rf: Arc<Mutex<RaftState>>,
        election: Arc<ElectionState>,
        votes: Vec<tokio::task::JoinHandle<Option<bool>>>,
        majority: usize,
        cancel_token: futures::channel::oneshot::Receiver<()>,
        new_log_entry: std::sync::mpsc::Sender<Option<Peer>>,
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
            // We are the leader now. The election timer can be stopped.
            election.stop_election_timer();

            rf.state = State::Leader;
            let log_len = rf.log.len();
            for item in rf.next_index.iter_mut() {
                *item = log_len;
            }
            for item in rf.match_index.iter_mut() {
                *item = 0;
            }
            for item in rf.current_step.iter_mut() {
                *item = 0;
            }
            // Sync all logs now.
            new_log_entry
                .send(None)
                .expect("Triggering log entry syncing should not fail");
            rf.persist();
        }
    }

    fn schedule_heartbeats(&self, interval: Duration) {
        for (peer_index, rpc_client) in self.peers.iter().enumerate() {
            if peer_index != self.me.0 {
                // rf is now owned by the outer async function.
                let rf = self.inner_state.clone();
                // RPC client must be cloned into the outer async function.
                let rpc_client = rpc_client.clone();
                // Shutdown signal.
                let keep_running = self.keep_running.clone();
                self.thread_pool.spawn(async move {
                    let mut interval = tokio::time::interval(interval);
                    while keep_running.load(Ordering::SeqCst) {
                        interval.tick().await;
                        if let Some(args) = Self::build_heartbeat(&rf) {
                            tokio::spawn(Self::send_heartbeat(
                                rpc_client.clone(),
                                args,
                            ));
                        }
                    }
                });
            }
        }
    }

    fn build_heartbeat(
        rf: &Arc<Mutex<RaftState>>,
    ) -> Option<AppendEntriesArgs> {
        let rf = rf.lock();

        if !rf.is_leader() {
            return None;
        }

        let (last_log_index, last_log_term) = rf.last_log_index_and_term();
        let args = AppendEntriesArgs {
            term: rf.current_term,
            leader_id: rf.leader_id,
            prev_log_index: last_log_index,
            prev_log_term: last_log_term,
            entries: vec![],
            leader_commit: rf.commit_index,
        };
        Some(args)
    }

    const HEARTBEAT_RETRY: usize = 3;
    async fn send_heartbeat(
        rpc_client: RpcClient,
        args: AppendEntriesArgs,
    ) -> std::io::Result<()> {
        retry_rpc(Self::HEARTBEAT_RETRY, move |_round| {
            rpc_client.clone().call_append_entries(args.clone())
        })
        .await?;
        Ok(())
    }

    fn run_log_entry_daemon(&mut self) -> std::thread::JoinHandle<()> {
        let (tx, rx) = std::sync::mpsc::channel::<Option<Peer>>();
        self.new_log_entry.replace(tx);

        // Clone everything that the thread needs.
        let this = self.clone();
        std::thread::spawn(move || {
            while let Ok(peer) = rx.recv() {
                if !this.keep_running.load(Ordering::SeqCst) {
                    break;
                }
                if !this.inner_state.lock().is_leader() {
                    continue;
                }
                for (i, rpc_client) in this.peers.iter().enumerate() {
                    if i != this.me.0 && peer.map(|p| p.0 == i).unwrap_or(true)
                    {
                        this.thread_pool.spawn(Self::sync_log_entry(
                            this.inner_state.clone(),
                            rpc_client.clone(),
                            i,
                            this.new_log_entry.clone().unwrap(),
                            this.apply_command_signal.clone(),
                        ));
                    }
                }
            }

            let stop_wait_group = this.stop_wait_group.clone();
            // Making sure the rest of `this` is dropped before the wait group.
            drop(this);
            drop(stop_wait_group);
        })
    }

    async fn sync_log_entry(
        rf: Arc<Mutex<RaftState>>,
        rpc_client: RpcClient,
        peer_index: usize,
        rerun: std::sync::mpsc::Sender<Option<Peer>>,
        apply_command_signal: Arc<Condvar>,
    ) {
        // TODO: cancel in flight changes?
        let args = Self::build_append_entries(&rf, peer_index);
        let term = args.term;
        let match_index = args.prev_log_index + args.entries.len();
        let succeeded = Self::append_entries(rpc_client, args).await;
        match succeeded {
            Ok(Some(succeeded)) => {
                if succeeded {
                    let mut rf = rf.lock();
                    rf.next_index[peer_index] = match_index + 1;
                    if match_index > rf.match_index[peer_index] {
                        rf.match_index[peer_index] = match_index;
                        if rf.is_leader() && rf.current_term == term {
                            let mut matched = rf.match_index.to_vec();
                            let mid = matched.len() / 2 + 1;
                            matched.sort();
                            let new_commit_index = matched[mid];
                            if new_commit_index > rf.commit_index
                                && rf.log[new_commit_index].term
                                    == rf.current_term
                            {
                                rf.commit_index = new_commit_index;
                                apply_command_signal.notify_one();
                            }
                        }
                    }
                } else {
                    let mut rf = rf.lock();

                    let step = &mut rf.current_step[peer_index];
                    if *step < 5 {
                        *step += 1;
                    }
                    let diff = (1 << 8) << *step;

                    let next_index = &mut rf.next_index[peer_index];
                    if diff >= *next_index {
                        *next_index = 1usize;
                    } else {
                        *next_index -= diff;
                    }

                    rerun
                        .send(Some(Peer(peer_index)))
                        .expect("Triggering log entry syncing should not fail");
                }
            }
            // Do nothing, not our term anymore.
            Ok(None) => {}
            Err(_) => {
                tokio::time::delay_for(Duration::from_millis(
                    HEARTBEAT_INTERVAL_MILLIS,
                ))
                .await;
                rerun
                    .send(Some(Peer(peer_index)))
                    .expect("Triggering log entry syncing should not fail");
            }
        };
    }

    fn build_append_entries(
        rf: &Arc<Mutex<RaftState>>,
        peer_index: usize,
    ) -> AppendEntriesArgs {
        let rf = rf.lock();
        let prev_log_index = rf.next_index[peer_index] - 1;
        let prev_log_term = rf.log[prev_log_index].term;
        AppendEntriesArgs {
            term: rf.current_term,
            leader_id: rf.leader_id,
            prev_log_index,
            prev_log_term,
            entries: rf.log[rf.next_index[peer_index]..].to_vec(),
            leader_commit: rf.commit_index,
        }
    }

    const APPEND_ENTRIES_RETRY: usize = 3;
    async fn append_entries(
        rpc_client: RpcClient,
        args: AppendEntriesArgs,
    ) -> std::io::Result<Option<bool>> {
        let term = args.term;
        let reply = retry_rpc(Self::APPEND_ENTRIES_RETRY, move |_round| {
            rpc_client.clone().call_append_entries(args.clone())
        })
        .await?;
        Ok(if reply.term == term {
            Some(reply.success)
        } else {
            None
        })
    }

    fn run_apply_command_daemon<Func>(
        &self,
        mut apply_command: Func,
    ) -> std::thread::JoinHandle<()>
    where
        Func: 'static + Send + FnMut(Index, Command),
    {
        let keep_running = self.keep_running.clone();
        let rf = self.inner_state.clone();
        let condvar = self.apply_command_signal.clone();
        let stop_wait_group = self.stop_wait_group.clone();
        std::thread::spawn(move || {
            while keep_running.load(Ordering::SeqCst) {
                let (mut index, commands) = {
                    let mut rf = rf.lock();
                    if rf.last_applied >= rf.commit_index {
                        condvar.wait_for(
                            &mut rf,
                            Duration::from_millis(HEARTBEAT_INTERVAL_MILLIS),
                        );
                    }
                    if rf.last_applied < rf.commit_index {
                        rf.last_applied += 1;
                        let index = rf.last_applied;
                        let commands: Vec<Command> = rf.log[index..]
                            .iter()
                            .map(|entry| entry.command.clone())
                            .collect();
                        (index, commands)
                    } else {
                        continue;
                    }
                };

                // Release the lock while calling external functions.
                for command in commands {
                    apply_command(index, command);
                    index += 1;
                }
            }

            drop(stop_wait_group);
        })
    }

    pub fn start(&self, command: Command) -> Option<(Term, Index)> {
        let mut rf = self.inner_state.lock();
        let term = rf.current_term;
        if !rf.is_leader() {
            return None;
        }

        let index = rf.log.len();
        rf.log.push(LogEntry {
            term,
            index,
            command,
        });
        rf.persist();

        self.new_log_entry
            .clone()
            .unwrap()
            .send(None)
            .expect("Sending to new log entry queue should never fail.");

        Some((term, index))
    }

    pub fn kill(mut self) {
        self.keep_running.store(false, Ordering::SeqCst);
        self.election.stop_election_timer();
        self.new_log_entry.take().map(|n| n.send(None));
        self.apply_command_signal.notify_all();
        self.stop_wait_group.wait();
        std::sync::Arc::try_unwrap(self.thread_pool)
            .expect(
                "All references to the thread pool should have been dropped.",
            )
            .shutdown_timeout(Duration::from_millis(
                HEARTBEAT_INTERVAL_MILLIS * 2,
            ));
        self.inner_state.lock().persist();
    }

    pub fn get_state(&self) -> (Term, bool) {
        let state = self.inner_state.lock();
        (state.current_term, state.is_leader())
    }
}

impl RaftState {
    fn persist(&self) {
        // TODO: implement
    }

    fn deferred_persist(&self) -> impl Drop + '_ {
        DropGuard::new(move || self.persist())
    }

    fn last_log_index_and_term(&self) -> (Index, Term) {
        let len = self.log.len();
        assert!(len > 0, "There should always be at least one entry in log");
        (len - 1, self.log.last().unwrap().term)
    }

    fn is_leader(&self) -> bool {
        self.state == State::Leader
    }
}

const HEARTBEAT_INTERVAL_MILLIS: u64 = 150;
const ELECTION_TIMEOUT_BASE_MILLIS: u64 = 150;
const ELECTION_TIMEOUT_VAR_MILLIS: u64 = 250;

impl ElectionState {
    fn reset_election_timer(&self) {
        let mut guard = self.timer.lock();
        guard.0 += 1;
        guard.1.replace(Instant::now() + Self::election_timeout());
        self.signal.notify_one();
    }

    fn try_reset_election_timer(&self, timer_count: usize) -> bool {
        let mut guard = self.timer.lock();
        if guard.0 != timer_count {
            return false;
        }
        guard.0 += 1;
        guard.1.replace(Instant::now() + Self::election_timeout());
        self.signal.notify_one();
        true
    }

    fn election_timeout() -> Duration {
        Duration::from_millis(
            ELECTION_TIMEOUT_BASE_MILLIS
                + thread_rng().gen_range(0, ELECTION_TIMEOUT_VAR_MILLIS),
        )
    }

    fn stop_election_timer(&self) {
        let mut guard = self.timer.lock();
        guard.0 += 1;
        guard.1.take();
        self.signal.notify_one();
    }
}

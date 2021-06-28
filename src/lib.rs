extern crate bincode;
extern crate futures_channel;
extern crate futures_util;
extern crate labrpc;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate tokio;

use std::convert::TryFrom;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_utils::sync::WaitGroup;
use parking_lot::{Condvar, Mutex};

use crate::apply_command::ApplyCommandFnMut;
pub use crate::apply_command::ApplyCommandMessage;
use crate::daemon_env::{DaemonEnv, ThreadEnv};
use crate::election::ElectionState;
use crate::index_term::IndexTerm;
use crate::persister::PersistedRaftState;
pub use crate::persister::Persister;
pub(crate) use crate::raft_state::RaftState;
pub(crate) use crate::raft_state::State;
pub use crate::rpcs::RpcClient;
pub use crate::snapshot::Snapshot;
use crate::snapshot::{RequestSnapshotFnMut, SnapshotDaemon};

mod apply_command;
mod daemon_env;
mod election;
mod heartbeats;
mod index_term;
mod install_snapshot;
mod log_array;
mod persister;
mod process_append_entries;
mod process_request_vote;
mod raft_state;
pub mod rpcs;
mod snapshot;
mod sync_log_entry;
pub mod utils;

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Term(pub usize);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Peer(usize);

pub type Index = usize;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LogEntry<Command> {
    index: Index,
    term: Term,
    command: Command,
}

#[derive(Clone)]
pub struct Raft<Command> {
    inner_state: Arc<Mutex<RaftState<Command>>>,
    peers: Vec<Arc<RpcClient>>,

    me: Peer,

    persister: Arc<dyn Persister>,

    new_log_entry: Option<std::sync::mpsc::Sender<Option<Peer>>>,
    apply_command_signal: Arc<Condvar>,
    keep_running: Arc<AtomicBool>,
    election: Arc<ElectionState>,
    snapshot_daemon: SnapshotDaemon,

    thread_pool: Arc<tokio::runtime::Runtime>,

    daemon_env: DaemonEnv,
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
struct AppendEntriesArgs<Command> {
    term: Term,
    leader_id: Peer,
    prev_log_index: Index,
    prev_log_term: Term,
    entries: Vec<LogEntry<Command>>,
    leader_commit: Index,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AppendEntriesReply {
    term: Term,
    success: bool,
    committed: Option<IndexTerm>,
}

// Commands must be
// 0. 'static: they have to live long enough for thread pools.
// 1. clone: they are put in vectors and request messages.
// 2. serializable: they are sent over RPCs and persisted.
// 3. deserializable: they are restored from storage.
// 4. send: they are referenced in futures.
// 5. default, because we need an element for the first entry.
impl<Command> Raft<Command>
where
    Command: 'static
        + Clone
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Default,
{
    /// Create a new raft instance.
    ///
    /// Each instance will create at least 3 + (number of peers) threads. The
    /// extensive usage of threads is to minimize latency.
    pub fn new(
        peers: Vec<RpcClient>,
        me: usize,
        persister: Arc<dyn Persister>,
        apply_command: impl ApplyCommandFnMut<Command>,
        max_state_size_bytes: Option<usize>,
        request_snapshot: impl RequestSnapshotFnMut,
    ) -> Self {
        let peer_size = peers.len();
        assert!(peer_size > me, "My index should be smaller than peer size.");
        let mut state = RaftState::create(peer_size, Peer(me));

        if let Ok(persisted_state) =
            PersistedRaftState::try_from(persister.read_state())
        {
            state.current_term = persisted_state.current_term;
            state.voted_for = persisted_state.voted_for;
            state.log = persisted_state.log;
            state.commit_index = state.log.start();
        }

        let election = ElectionState::create();
        election.reset_election_timer();

        let daemon_env = DaemonEnv::create();
        let thread_env = daemon_env.for_thread();
        let thread_pool = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .thread_name(format!("raft-instance-{}", me))
            .worker_threads(peer_size)
            .on_thread_start(move || thread_env.clone().attach())
            .on_thread_stop(ThreadEnv::detach)
            .build()
            .expect("Creating thread pool should not fail");
        let peers = peers.into_iter().map(Arc::new).collect();
        let mut this = Raft {
            inner_state: Arc::new(Mutex::new(state)),
            peers,
            me: Peer(me),
            persister,
            new_log_entry: None,
            apply_command_signal: Arc::new(Default::default()),
            keep_running: Arc::new(Default::default()),
            election: Arc::new(election),
            snapshot_daemon: Default::default(),
            thread_pool: Arc::new(thread_pool),
            daemon_env,
            stop_wait_group: WaitGroup::new(),
        };

        this.keep_running.store(true, Ordering::SeqCst);
        // Running in a standalone thread.
        this.run_snapshot_daemon(max_state_size_bytes, request_snapshot);
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
}

// Command must be
// 0. 'static: Raft<Command> must be 'static, it is moved to another thread.
// 1. clone: they are copied to the persister.
// 2. send: Arc<Mutex<Vec<LogEntry<Command>>>> must be send, it is moved to another thread.
// 3. serialize: they are converted to bytes to persist.
// 4. default: a default value is used as the first element of log.
impl<Command> Raft<Command>
where
    Command: 'static + Clone + Send + serde::Serialize + Default,
{
    pub fn start(&self, command: Command) -> Option<(Term, Index)> {
        let mut rf = self.inner_state.lock();
        let term = rf.current_term;
        if !rf.is_leader() {
            return None;
        }

        let index = rf.log.add_command(term, command);
        self.persister.save_state(rf.persisted_state().into());

        let _ = self.new_log_entry.clone().unwrap().send(None);

        Some((term, index))
    }

    pub fn kill(mut self) {
        self.keep_running.store(false, Ordering::SeqCst);
        self.election.stop_election_timer();
        self.new_log_entry.take().map(|n| n.send(None));
        self.apply_command_signal.notify_all();
        self.snapshot_daemon.kill();
        self.stop_wait_group.wait();
        std::sync::Arc::try_unwrap(self.thread_pool)
            .expect(
                "All references to the thread pool should have been dropped.",
            )
            .shutdown_timeout(Duration::from_millis(
                HEARTBEAT_INTERVAL_MILLIS * 2,
            ));
        // DaemonEnv must be shutdown after the thread pool, since there might
        // be tasks logging errors in the pool.
        self.daemon_env.shutdown();
    }

    pub fn get_state(&self) -> (Term, bool) {
        let state = self.inner_state.lock();
        (state.current_term, state.is_leader())
    }
}

pub(crate) const HEARTBEAT_INTERVAL_MILLIS: u64 = 150;
const RPC_DEADLINE: Duration = Duration::from_secs(2);

impl<C> Raft<C> {
    pub const NO_SNAPSHOT: fn(Index) = |_| {};
}

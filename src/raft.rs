use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_utils::sync::WaitGroup;
use parking_lot::{Condvar, Mutex};
use serde_derive::{Deserialize, Serialize};

use crate::apply_command::ApplyCommandFnMut;
use crate::daemon_env::{DaemonEnv, ThreadEnv};
use crate::election::ElectionState;
use crate::heartbeats::{HeartbeatsDaemon, HEARTBEAT_INTERVAL};
use crate::persister::PersistedRaftState;
use crate::snapshot::{RequestSnapshotFnMut, SnapshotDaemon};
use crate::verify_authority::VerifyAuthorityDaemon;
use crate::{
    utils, IndexTerm, Persister, RaftState, RemoteRaft, ReplicableCommand,
};

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Term(pub usize);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Peer(pub usize);

#[derive(Clone)]
pub struct Raft<Command> {
    pub(crate) inner_state: Arc<Mutex<RaftState<Command>>>,
    pub(crate) peers: Vec<Arc<dyn RemoteRaft<Command>>>,

    pub(crate) me: Peer,

    pub(crate) persister: Arc<dyn Persister>,

    pub(crate) new_log_entry: Option<utils::SharedSender<Option<Peer>>>,
    pub(crate) apply_command_signal: Arc<Condvar>,
    pub(crate) keep_running: Arc<AtomicBool>,
    pub(crate) election: Arc<ElectionState>,
    pub(crate) snapshot_daemon: SnapshotDaemon,
    pub(crate) verify_authority_daemon: VerifyAuthorityDaemon,
    pub(crate) heartbeats_daemon: HeartbeatsDaemon,

    pub(crate) thread_pool: utils::ThreadPoolHolder,

    pub(crate) daemon_env: DaemonEnv,
    pub(crate) stop_wait_group: WaitGroup,
}

impl<Command: ReplicableCommand> Raft<Command> {
    /// Create a new raft instance.
    ///
    /// Each instance will create at least 4 + (number of peers) threads. The
    /// extensive usage of threads is to minimize latency.
    pub fn new(
        peers: Vec<impl RemoteRaft<Command> + 'static>,
        me: usize,
        persister: Arc<dyn Persister>,
        apply_command: impl ApplyCommandFnMut<Command>,
        max_state_size_bytes: Option<usize>,
        request_snapshot: impl RequestSnapshotFnMut,
    ) -> Self {
        let peer_size = peers.len();
        assert!(peer_size > me, "My index should be smaller than peer size.");
        let mut state = RaftState::create(peer_size, Peer(me));
        // COMMIT_INDEX_INVARIANT, SNAPSHOT_INDEX_INVARIANT: Initially
        // commit_index = log.start() and commit_index + 1 = log.end(). Thus
        // log.start() <= commit_index and commit_index < log.end() both hold.
        assert_eq!(state.commit_index + 1, state.log.end());

        if let Ok(persisted_state) =
            PersistedRaftState::try_from(persister.read_state())
        {
            state.current_term = persisted_state.current_term;
            state.voted_for = persisted_state.voted_for;
            state.log = persisted_state.log;
            state.commit_index = state.log.start();
            // COMMIT_INDEX_INVARIANT, SNAPSHOT_INDEX_INVARIANT: the saved
            // snapshot must have a valid log.start() and log.end(). Thus
            // log.start() <= commit_index and commit_index < log.end() hold.
            assert!(state.commit_index < state.log.end());

            state
                .log
                .validate(state.current_term)
                .expect("Persisted log should not contain error");
        }

        let election = ElectionState::create();
        election.reset_election_timer();

        let daemon_env = DaemonEnv::create();
        let thread_env = daemon_env.for_thread();
        let thread_pool = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .thread_name(format!("raft-instance-{}", me))
            .worker_threads(peer_size)
            .on_thread_start(move || thread_env.clone().attach())
            .on_thread_stop(ThreadEnv::detach)
            .build()
            .expect("Creating thread pool should not fail");
        let peers = peers
            .into_iter()
            .map(|r| Arc::new(r) as Arc<dyn RemoteRaft<Command>>)
            .collect();

        let mut this = Raft {
            inner_state: Arc::new(Mutex::new(state)),
            peers,
            me: Peer(me),
            persister,
            new_log_entry: None,
            apply_command_signal: Arc::new(Condvar::new()),
            keep_running: Arc::new(AtomicBool::new(true)),
            election: Arc::new(election),
            snapshot_daemon: SnapshotDaemon::create(),
            verify_authority_daemon: VerifyAuthorityDaemon::create(peer_size),
            heartbeats_daemon: HeartbeatsDaemon::create(),
            thread_pool: utils::ThreadPoolHolder::new(thread_pool),
            daemon_env,
            stop_wait_group: WaitGroup::new(),
        };

        // Running in a standalone thread.
        this.run_verify_authority_daemon();
        // Running in a standalone thread.
        this.run_snapshot_daemon(max_state_size_bytes, request_snapshot);
        // Running in a standalone thread.
        this.run_log_entry_daemon();
        // Running in a standalone thread.
        this.run_apply_command_daemon(apply_command);
        // One off function that schedules many little tasks, running on the
        // internal thread pool.
        this.schedule_heartbeats(HEARTBEAT_INTERVAL);
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
impl<Command: ReplicableCommand> Raft<Command> {
    /// Adds a new command to the log, returns its index and the current term.
    ///
    /// Returns `None` if we are not the leader. The log entry may not have been
    /// committed to the log when this method returns. When and if it is
    /// committed, the `apply_command` callback will be called.
    pub fn start(&self, command: Command) -> Option<IndexTerm> {
        let mut rf = self.inner_state.lock();
        let term = rf.current_term;
        if !rf.is_leader() {
            return None;
        }

        let index = rf.log.add_command(term, command);
        self.persister.save_state(rf.persisted_state().into());

        // Several attempts have been made to remove the unwrap below.
        let _ = self.new_log_entry.as_ref().unwrap().send(None);

        log::info!("{:?} started new entry at {} {:?}", self.me, index, term);
        Some(IndexTerm::pack(index, term))
    }

    /// Cleanly shutdown this instance. This function never blocks forever. It
    /// either panics or returns eventually.
    pub fn kill(mut self) {
        self.keep_running.store(false, Ordering::SeqCst);
        self.election.stop_election_timer();
        self.new_log_entry.take().map(|n| n.send(None));
        self.apply_command_signal.notify_all();
        self.snapshot_daemon.kill();
        self.verify_authority_daemon.kill();
        // We cannot easily combine stop_wait_group into DaemonEnv because of
        // shutdown dependencies. The thread pool is not managed by DaemonEnv,
        // but it cannot be shutdown until all daemons are. On the other hand
        // the thread pool uses DaemonEnv, thus must be shutdown before
        // DaemonEnv. The shutdown sequence is stop_wait_group -> thread_pool
        // -> DaemonEnv. The first and third cannot be combined with the second
        // in the middle.
        self.stop_wait_group.wait();
        self.thread_pool
            .take()
            .expect(
                "All references to the thread pool should have been dropped.",
            )
            .shutdown_timeout(HEARTBEAT_INTERVAL * 2);
        // DaemonEnv must be shutdown after the thread pool, since there might
        // be tasks logging errors in the pool.
        self.daemon_env.shutdown();
    }

    /// Returns the current term and whether we are the leader.
    ///
    /// Take a quick peek at the current state of this instance. The returned
    /// value is stale as soon as this function returns.
    pub fn get_state(&self) -> (Term, bool) {
        let state = self.inner_state.lock();
        (state.current_term, state.is_leader())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_raft_must_sync() {
        let optional_raft: Option<super::Raft<i32>> = None;

        fn must_sync<T: Sync>(value: T) {
            drop(value)
        }
        must_sync(optional_raft)
        // The following raft is not Sync.
        // let optional_raft: Option<super::Raft<std::rc::Rc<i32>>> = None;
    }
}

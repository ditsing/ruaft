use crossbeam_utils::sync::WaitGroup;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use serde_derive::{Deserialize, Serialize};

use crate::apply_command::ApplyCommandFnMut;
use crate::daemon_env::{DaemonEnv, ThreadEnv};
use crate::daemon_watch::{Daemon, DaemonWatch};
use crate::election::ElectionState;
use crate::heartbeats::{HeartbeatsDaemon, HEARTBEAT_INTERVAL};
use crate::persister::PersistedRaftState;
use crate::remote_context::RemoteContext;
use crate::remote_peer::RemotePeer;
use crate::snapshot::{RequestSnapshotFnMut, SnapshotDaemon};
use crate::sync_log_entries::SyncLogEntriesComms;
use crate::term_marker::TermMarker;
use crate::verify_authority::VerifyAuthorityDaemon;
use crate::{IndexTerm, Persister, RaftState, RemoteRaft, ReplicableCommand};

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Term(pub usize);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Peer(pub usize);

#[derive(Clone)]
pub struct Raft<Command> {
    pub(crate) inner_state: Arc<Mutex<RaftState<Command>>>,
    pub(crate) peers: Vec<Peer>,

    pub(crate) me: Peer,

    pub(crate) persister: Arc<dyn Persister>,

    pub(crate) sync_log_entries_comms: SyncLogEntriesComms,
    pub(crate) apply_command_signal: Arc<Condvar>,
    pub(crate) keep_running: Arc<AtomicBool>,
    pub(crate) election: Arc<ElectionState>,
    pub(crate) snapshot_daemon: SnapshotDaemon,
    pub(crate) verify_authority_daemon: VerifyAuthorityDaemon,
    pub(crate) heartbeats_daemon: HeartbeatsDaemon,

    pub(crate) thread_pool: tokio::runtime::Handle,
    pub(crate) daemon_env: DaemonEnv,

    stop_wait_group: WaitGroup,
    join_handle: Arc<Mutex<Option<RaftJoinHandle>>>,
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

        let inner_state = Arc::new(Mutex::new(state));
        let election = Arc::new(ElectionState::create());
        election.reset_election_timer();

        let term_marker = TermMarker::create(
            inner_state.clone(),
            election.clone(),
            persister.clone(),
        );

        let verify_authority_daemon = VerifyAuthorityDaemon::create(peer_size);
        let remote_peers = peers
            .into_iter()
            .enumerate()
            .map(|(index, remote_raft)| {
                RemotePeer::create(
                    Peer(index),
                    remote_raft,
                    verify_authority_daemon.beat_ticker(index),
                )
            })
            .collect();

        let context = RemoteContext::create(term_marker, remote_peers);

        let daemon_env = DaemonEnv::create();
        let thread_env = daemon_env.for_thread();
        let thread_pool = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .thread_name(format!("raft-instance-{}", me))
            .worker_threads(peer_size)
            .on_thread_start(move || {
                context.clone().attach();
                thread_env.clone().attach();
            })
            .on_thread_stop(move || {
                RemoteContext::<Command>::detach();
                ThreadEnv::detach();
            })
            .build()
            .expect("Creating thread pool should not fail");
        let peers = (0..peer_size).map(Peer).collect();
        let (sync_log_entries_comms, sync_log_entries_daemon) =
            crate::sync_log_entries::create(peer_size);

        let mut this = Raft {
            inner_state,
            peers,
            me: Peer(me),
            persister,
            sync_log_entries_comms,
            apply_command_signal: Arc::new(Condvar::new()),
            keep_running: Arc::new(AtomicBool::new(true)),
            election,
            snapshot_daemon: SnapshotDaemon::create(),
            verify_authority_daemon,
            heartbeats_daemon: HeartbeatsDaemon::create(),
            thread_pool: thread_pool.handle().clone(),
            stop_wait_group: WaitGroup::new(),
            daemon_env: daemon_env.clone(),
            // The join handle will be created later.
            join_handle: Arc::new(Mutex::new(None)),
        };

        let mut daemon_watch = DaemonWatch::create(daemon_env.for_thread());
        // Running in a standalone thread.
        let verify_authority_daemon = this.run_verify_authority_daemon();
        daemon_watch
            .create_daemon(Daemon::VerifyAuthority, verify_authority_daemon);
        // Running in a standalone thread.
        let snapshot_daemon =
            this.run_snapshot_daemon(max_state_size_bytes, request_snapshot);
        daemon_watch.create_daemon(Daemon::Snapshot, snapshot_daemon);
        // Running in a standalone thread.
        let sync_log_entry_daemon =
            this.run_log_entry_daemon(sync_log_entries_daemon);
        daemon_watch
            .create_daemon(Daemon::SyncLogEntries, sync_log_entry_daemon);
        // Running in a standalone thread.
        let apply_command_daemon = this.run_apply_command_daemon(apply_command);
        daemon_watch.create_daemon(Daemon::ApplyCommand, apply_command_daemon);
        // One off function that schedules many little tasks, running on the
        // internal thread pool.
        this.schedule_heartbeats(HEARTBEAT_INTERVAL);
        // The last step is to start running election timer.
        daemon_watch.create_daemon(Daemon::ElectionTimer, {
            let raft = this.clone();
            move || raft.run_election_timer()
        });

        // Create the join handle
        this.join_handle.lock().replace(RaftJoinHandle {
            stop_wait_group: this.stop_wait_group.clone(),
            thread_pool,
            daemon_watch,
            daemon_env,
        });

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
        let _guard = self.daemon_env.for_scope();

        let mut rf = self.inner_state.lock();
        let term = rf.current_term;
        if !rf.is_leader() {
            return None;
        }

        let index = rf.log.add_command(term, command);
        self.persister.save_state(rf.persisted_state().into());

        self.sync_log_entries_comms.update_followers(index);

        log::info!("{:?} started new entry at {} {:?}", self.me, index, term);
        Some(IndexTerm::pack(index, term))
    }

    /// Cleanly shutdown this instance. This function never blocks forever. It
    /// either panics or returns eventually.
    pub fn kill(self) -> RaftJoinHandle {
        self.keep_running.store(false, Ordering::Release);
        self.election.stop_election_timer();
        self.sync_log_entries_comms.kill();
        self.apply_command_signal.notify_all();
        self.snapshot_daemon.kill();
        self.verify_authority_daemon.kill();

        self.join_handle.lock().take().unwrap()
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

/// A join handle returned by `Raft::kill()`. Join this handle to cleanly
/// shutdown a Raft instance.
///
/// All clones of the same Raft instance created by `Raft::clone()` must be
/// dropped before `RaftJoinHandle::join()` can return.
///
/// After `RaftJoinHandle::join()` returns, all threads and thread pools created
/// by this Raft instance will have stopped. No callbacks will be called. No new
/// commits will be created by this Raft instance.
#[must_use]
pub struct RaftJoinHandle {
    stop_wait_group: WaitGroup,
    thread_pool: tokio::runtime::Runtime,
    daemon_watch: DaemonWatch,
    daemon_env: DaemonEnv,
}

impl RaftJoinHandle {
    const SHUTDOWN_TIMEOUT: Duration =
        Duration::from_millis(HEARTBEAT_INTERVAL.as_millis() as u64 * 2);

    /// Waits for the Raft instance to shutdown.
    ///
    /// See the struct documentation for more details.
    pub fn join(self) {
        // Wait for all Raft instances to be dropped.
        self.stop_wait_group.wait();
        self.daemon_watch.wait_for_daemons();
        self.thread_pool.shutdown_timeout(Self::SHUTDOWN_TIMEOUT);
        // DaemonEnv must be shutdown after the thread pool, since there might
        // be tasks logging errors in the pool.
        self.daemon_env.shutdown();
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

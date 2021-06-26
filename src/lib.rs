extern crate bincode;
extern crate futures_channel;
extern crate futures_util;
extern crate labrpc;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate tokio;

use std::convert::TryFrom;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_utils::sync::WaitGroup;
use parking_lot::{Condvar, Mutex};

use crate::apply_command::ApplyCommandFnMut;
pub use crate::apply_command::ApplyCommandMessage;
use crate::daemon_env::{DaemonEnv, ErrorKind, ThreadEnv};
use crate::election::ElectionState;
use crate::index_term::IndexTerm;
use crate::install_snapshot::InstallSnapshotArgs;
use crate::persister::PersistedRaftState;
pub use crate::persister::Persister;
pub(crate) use crate::raft_state::RaftState;
pub(crate) use crate::raft_state::State;
pub use crate::rpcs::RpcClient;
pub use crate::snapshot::Snapshot;
use crate::snapshot::{RequestSnapshotFnMut, SnapshotDaemon};
use crate::utils::retry_rpc;

mod apply_command;
mod daemon_env;
mod election;
mod index_term;
mod install_snapshot;
mod log_array;
mod persister;
mod raft_state;
pub mod rpcs;
mod snapshot;
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

#[repr(align(64))]
struct Opening(Arc<AtomicUsize>);

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
// 1. clone: they are copied to the persister.
// 2. serialize: they are converted to bytes to persist.
// 3. default: a default value is used as the first element of the log.
impl<Command> Raft<Command>
where
    Command: Clone + serde::Serialize + Default,
{
    pub(crate) fn process_request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> RequestVoteReply {
        // Note: do not change this to `let _ = ...`.
        let _guard = self.daemon_env.for_scope();

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
            self.persister.save_state(rf.persisted_state().into());
        }

        let voted_for = rf.voted_for;
        let last_log = rf.log.last_index_term();
        if (voted_for.is_none() || voted_for == Some(args.candidate_id))
            && (args.last_log_term > last_log.term
                || (args.last_log_term == last_log.term
                    && args.last_log_index >= last_log.index))
        {
            rf.voted_for = Some(args.candidate_id);

            // It is possible that we have set a timer above when updating the
            // current term. It does not hurt to update the timer again.
            // We do need to persist, though.
            self.election.reset_election_timer();
            self.persister.save_state(rf.persisted_state().into());

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
        args: AppendEntriesArgs<Command>,
    ) -> AppendEntriesReply {
        // Note: do not change this to `let _ = ...`.
        let _guard = self.daemon_env.for_scope();

        let mut rf = self.inner_state.lock();
        if rf.current_term > args.term {
            return AppendEntriesReply {
                term: rf.current_term,
                success: false,
                committed: Some(rf.log.first_after(rf.commit_index).into()),
            };
        }

        if rf.current_term < args.term {
            rf.current_term = args.term;
            rf.voted_for = None;
            self.persister.save_state(rf.persisted_state().into());
        }

        rf.state = State::Follower;
        rf.leader_id = args.leader_id;

        self.election.reset_election_timer();

        if rf.log.start() > args.prev_log_index
            || rf.log.end() <= args.prev_log_index
            || rf.log[args.prev_log_index].term != args.prev_log_term
        {
            return AppendEntriesReply {
                term: args.term,
                success: args.prev_log_index < rf.log.start(),
                committed: Some(rf.log.first_after(rf.commit_index).into()),
            };
        }

        for (i, entry) in args.entries.iter().enumerate() {
            let index = i + args.prev_log_index + 1;
            if rf.log.end() > index {
                if rf.log[index].term != entry.term {
                    check_or_record!(
                        index > rf.commit_index,
                        ErrorKind::RollbackCommitted(index),
                        "Entries before commit index should never be rolled back",
                        &rf
                    );
                    rf.log.truncate(index);
                    rf.log.push(entry.clone());
                }
            } else {
                rf.log.push(entry.clone());
            }
        }

        self.persister.save_state(rf.persisted_state().into());

        if args.leader_commit > rf.commit_index {
            rf.commit_index = if args.leader_commit < rf.log.end() {
                args.leader_commit
            } else {
                rf.log.last_index_term().index
            };
            self.apply_command_signal.notify_one();
        }
        self.snapshot_daemon.log_grow(rf.log.start(), rf.log.end());

        AppendEntriesReply {
            term: args.term,
            success: true,
            committed: None,
        }
    }
}

enum SyncLogEntryOperation<Command> {
    AppendEntries(AppendEntriesArgs<Command>),
    InstallSnapshot(InstallSnapshotArgs),
    None,
}

enum SyncLogEntryResult {
    TermElapsed(Term),
    Archived(IndexTerm),
    Diverged(IndexTerm),
    Success,
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
        rf: &Mutex<RaftState<Command>>,
    ) -> Option<AppendEntriesArgs<Command>> {
        let rf = rf.lock();

        if !rf.is_leader() {
            return None;
        }

        let last_log = rf.log.last_index_term();
        let args = AppendEntriesArgs {
            term: rf.current_term,
            leader_id: rf.leader_id,
            prev_log_index: last_log.index,
            prev_log_term: last_log.term,
            entries: vec![],
            leader_commit: rf.commit_index,
        };
        Some(args)
    }

    const HEARTBEAT_RETRY: usize = 1;
    async fn send_heartbeat(
        rpc_client: Arc<RpcClient>,
        args: AppendEntriesArgs<Command>,
    ) -> std::io::Result<()> {
        // Passing a reference that is moved to the following closure.
        //
        // It won't work if the rpc_client of type Arc is moved into the closure
        // directly. To clone the Arc, the function must own a mutable reference
        // to it. At the same time, rpc_client.call_append_entries() returns a
        // future that must own a reference, too. That caused a compiling error
        // of FnMut allowing "references to captured variables to escape".
        //
        // By passing-in a reference instead of an Arc, the closure becomes a Fn
        // (no Mut), which can allow references to escape.
        //
        // Another option is to use non-move closures, in which case rpc_client
        // of type Arc can be passed-in directly. However that requires args to
        // be sync because they can be shared by more than one futures.
        let rpc_client = rpc_client.as_ref();
        retry_rpc(Self::HEARTBEAT_RETRY, RPC_DEADLINE, move |_round| {
            rpc_client.call_append_entries(args.clone())
        })
        .await?;
        Ok(())
    }

    fn run_log_entry_daemon(&mut self) {
        let (tx, rx) = std::sync::mpsc::channel::<Option<Peer>>();
        self.new_log_entry.replace(tx);

        // Clone everything that the thread needs.
        let this = self.clone();
        let join_handle = std::thread::spawn(move || {
            // Note: do not change this to `let _ = ...`.
            let _guard = this.daemon_env.for_scope();

            let mut openings = vec![];
            openings.resize_with(this.peers.len(), || {
                Opening(Arc::new(AtomicUsize::new(0)))
            });
            let openings = openings; // Not mutable beyond this point.

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
                        // Only schedule a new task if the last task has cleared
                        // the queue of RPC requests.
                        if openings[i].0.fetch_add(1, Ordering::SeqCst) == 0 {
                            this.thread_pool.spawn(Self::sync_log_entry(
                                this.inner_state.clone(),
                                rpc_client.clone(),
                                i,
                                this.new_log_entry.clone().unwrap(),
                                openings[i].0.clone(),
                                this.apply_command_signal.clone(),
                            ));
                        }
                    }
                }
            }

            let stop_wait_group = this.stop_wait_group.clone();
            // Making sure the rest of `this` is dropped before the wait group.
            drop(this);
            drop(stop_wait_group);
        });
        self.daemon_env.watch_daemon(join_handle);
    }

    async fn sync_log_entry(
        rf: Arc<Mutex<RaftState<Command>>>,
        rpc_client: Arc<RpcClient>,
        peer_index: usize,
        rerun: std::sync::mpsc::Sender<Option<Peer>>,
        opening: Arc<AtomicUsize>,
        apply_command_signal: Arc<Condvar>,
    ) {
        if opening.swap(0, Ordering::SeqCst) == 0 {
            return;
        }

        let operation = Self::build_sync_log_entry(&rf, peer_index);
        let (term, prev_log_index, match_index, succeeded) = match operation {
            SyncLogEntryOperation::AppendEntries(args) => {
                let term = args.term;
                let prev_log_index = args.prev_log_index;
                let match_index = args.prev_log_index + args.entries.len();
                let succeeded = Self::append_entries(&rpc_client, args).await;

                (term, prev_log_index, match_index, succeeded)
            }
            SyncLogEntryOperation::InstallSnapshot(args) => {
                let term = args.term;
                let prev_log_index = args.last_included_index;
                let match_index = args.last_included_index;
                let succeeded =
                    Self::send_install_snapshot(&rpc_client, args).await;

                (term, prev_log_index, match_index, succeeded)
            }
            SyncLogEntryOperation::None => return,
        };

        let peer = Peer(peer_index);
        match succeeded {
            Ok(SyncLogEntryResult::Success) => {
                let mut rf = rf.lock();

                if rf.current_term != term {
                    return;
                }

                rf.next_index[peer_index] = match_index + 1;
                rf.current_step[peer_index] = 0;
                if match_index > rf.match_index[peer_index] {
                    rf.match_index[peer_index] = match_index;
                    if rf.is_leader() && rf.current_term == term {
                        let mut matched = rf.match_index.to_vec();
                        let mid = matched.len() / 2 + 1;
                        matched.sort_unstable();
                        let new_commit_index = matched[mid];
                        if new_commit_index > rf.commit_index
                            && rf.log[new_commit_index].term == rf.current_term
                        {
                            rf.commit_index = new_commit_index;
                            apply_command_signal.notify_one();
                        }
                    }
                }
            }
            Ok(SyncLogEntryResult::Archived(committed)) => {
                if prev_log_index >= committed.index {
                    eprintln!(
                        "Peer {} misbehaves: send prev log index {}, got committed {:?}",
                        peer_index, prev_log_index, committed
                    );
                }

                let mut rf = rf.lock();
                Self::check_committed(&rf, peer, committed.clone());

                rf.current_step[peer_index] = 0;
                rf.next_index[peer_index] = committed.index;

                // Ignore the error. The log syncing thread must have died.
                let _ = rerun.send(Some(Peer(peer_index)));
            }
            Ok(SyncLogEntryResult::Diverged(committed)) => {
                if prev_log_index < committed.index {
                    eprintln!(
                        "Peer {} misbehaves: diverged at {}, but committed {:?}",
                        peer_index, prev_log_index, committed
                    );
                }
                let mut rf = rf.lock();
                Self::check_committed(&rf, peer, committed.clone());

                let step = &mut rf.current_step[peer_index];
                if *step < 5 {
                    *step += 1;
                }
                let diff = 4 << *step;

                let next_index = &mut rf.next_index[peer_index];
                if diff >= *next_index {
                    *next_index = 1usize;
                } else {
                    *next_index -= diff;
                }

                if *next_index < committed.index {
                    *next_index = committed.index;
                }

                // Ignore the error. The log syncing thread must have died.
                let _ = rerun.send(Some(Peer(peer_index)));
            }
            // Do nothing, not our term anymore.
            Ok(SyncLogEntryResult::TermElapsed(_)) => {}
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(
                    HEARTBEAT_INTERVAL_MILLIS,
                ))
                .await;
                // Ignore the error. The log syncing thread must have died.
                let _ = rerun.send(Some(Peer(peer_index)));
            }
        };
    }

    fn check_committed(
        rf: &RaftState<Command>,
        peer: Peer,
        committed: IndexTerm,
    ) {
        if committed.index < rf.log.start() {
            return;
        }
        let local_term = rf.log.at(committed.index).term;
        if committed.term != local_term {
            eprintln!(
                "{:?} committed log diverged at {:?}: {:?} v.s. leader {:?}",
                peer, committed.index, committed.term, local_term
            );
        }
    }

    fn build_sync_log_entry(
        rf: &Mutex<RaftState<Command>>,
        peer_index: usize,
    ) -> SyncLogEntryOperation<Command> {
        let rf = rf.lock();
        if !rf.is_leader() {
            return SyncLogEntryOperation::None;
        }

        // To send AppendEntries request, next_index must be strictly larger
        // than start(). Otherwise we won't be able to know the log term of the
        // entry right before next_index.
        if rf.next_index[peer_index] > rf.log.start() {
            SyncLogEntryOperation::AppendEntries(Self::build_append_entries(
                &rf, peer_index,
            ))
        } else {
            SyncLogEntryOperation::InstallSnapshot(
                Self::build_install_snapshot(&rf),
            )
        }
    }

    fn build_append_entries(
        rf: &RaftState<Command>,
        peer_index: usize,
    ) -> AppendEntriesArgs<Command> {
        let prev_log_index = rf.next_index[peer_index] - 1;
        let prev_log_term = rf.log[prev_log_index].term;
        AppendEntriesArgs {
            term: rf.current_term,
            leader_id: rf.leader_id,
            prev_log_index,
            prev_log_term,
            entries: rf.log.after(rf.next_index[peer_index]).to_vec(),
            leader_commit: rf.commit_index,
        }
    }

    const APPEND_ENTRIES_RETRY: usize = 1;
    async fn append_entries(
        rpc_client: &RpcClient,
        args: AppendEntriesArgs<Command>,
    ) -> std::io::Result<SyncLogEntryResult> {
        let term = args.term;
        let reply = retry_rpc(
            Self::APPEND_ENTRIES_RETRY,
            RPC_DEADLINE,
            move |_round| rpc_client.call_append_entries(args.clone()),
        )
        .await?;
        Ok(if reply.term == term {
            if let Some(committed) = reply.committed {
                if reply.success {
                    SyncLogEntryResult::Archived(committed)
                } else {
                    SyncLogEntryResult::Diverged(committed)
                }
            } else {
                SyncLogEntryResult::Success
            }
        } else {
            SyncLogEntryResult::TermElapsed(reply.term)
        })
    }

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

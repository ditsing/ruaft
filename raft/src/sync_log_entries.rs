use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::daemon_env::ErrorKind;
use crate::heartbeats::HEARTBEAT_INTERVAL;
use crate::peer_progress::PeerProgress;
use crate::remote::RemoteContext;
use crate::utils::{retry_rpc, SharedSender, RPC_DEADLINE};
use crate::{
    check_or_record, AppendEntriesArgs, Index, IndexTerm, InstallSnapshotArgs,
    Peer, Raft, RaftState, ReplicableCommand, Term,
};

#[derive(Eq, PartialEq)]
enum Event {
    NewTerm(Term, Index),
    NewLogEntry(Index),
    Rerun(Peer),
    Shutdown,
}

impl Event {
    fn should_schedule(&self, peer: Peer) -> bool {
        match self {
            Event::NewTerm(..) => true,
            Event::NewLogEntry(_index) => true,
            Event::Rerun(p) => p == &peer,
            Event::Shutdown => false,
        }
    }
}

#[derive(Clone)]
pub(crate) struct SyncLogEntriesComms {
    tx: SharedSender<Event>,
}

impl SyncLogEntriesComms {
    pub fn update_followers(&self, index: Index) {
        // Ignore the error. The log syncing thread must have died.
        let _ = self.tx.send(Event::NewLogEntry(index));
    }

    pub fn reset_progress(&self, term: Term, index: Index) {
        let index = std::cmp::max(index, 1);
        let _ = self.tx.send(Event::NewTerm(term, index));
    }

    pub fn kill(&self) {
        // The sync log entry daemon might have exited.
        let _ = self.tx.send(Event::Shutdown);
    }

    fn rerun(&self, peer: Peer) {
        // Ignore the error. The log syncing thread must have died.
        let _ = self.tx.send(Event::Rerun(peer));
    }
}

pub(crate) struct SyncLogEntriesDaemon {
    rx: std::sync::mpsc::Receiver<Event>,
    peer_progress: Vec<PeerProgress>,
}

pub(crate) fn create(
    peer_size: usize,
) -> (SyncLogEntriesComms, SyncLogEntriesDaemon) {
    let (tx, rx) = std::sync::mpsc::channel();
    let tx = SharedSender::new(tx);
    let peer_progress = (0..peer_size).map(PeerProgress::create).collect();
    (
        SyncLogEntriesComms { tx },
        SyncLogEntriesDaemon { rx, peer_progress },
    )
}

enum SyncLogEntriesOperation<Command> {
    AppendEntries(AppendEntriesArgs<Command>),
    InstallSnapshot(InstallSnapshotArgs),
    None,
}

enum SyncLogEntriesResult {
    TermElapsed(Term),
    Archived(IndexTerm),
    Diverged(IndexTerm),
    Success,
}

#[derive(Clone, Copy, Debug)]
struct TaskNumber(usize);

// Command must be
// 0. 'static: Raft<Command> must be 'static, it is moved to another thread.
// 1. clone: they are copied to the persister.
// 2. send: Arc<Mutex<Vec<LogEntry<Command>>>> must be send, it is moved to another thread.
// 3. serialize: they are converted to bytes to persist.
impl<Command: ReplicableCommand> Raft<Command> {
    /// Runs a daemon thread that syncs log entries to peers.
    ///
    /// This daemon watches the `new_log_entry` channel. Each item delivered by
    /// the channel is a request to sync log entries to either a peer
    /// (`Some(peer_index)`) or all peers (`None`).
    ///
    /// The daemon tries to collapse requests about the same peer together. A
    /// new task is only scheduled when the pending requests number turns from
    /// zero to one. Even then there could still be more than one tasks syncing
    /// logs to the same peer at the same time.
    ///
    /// New tasks will still be scheduled when we are not the leader. The task
    /// will exist without not do anything in that case.
    ///
    /// See comments on [`Raft::sync_log_entries`] to learn about the syncing
    /// and backoff strategy.
    pub(crate) fn run_log_entry_daemon(
        &self,
        SyncLogEntriesDaemon { rx, peer_progress }: SyncLogEntriesDaemon,
    ) -> impl FnOnce() {
        // Clone everything that the thread needs.
        let this = self.clone();
        move || {
            log::info!("{:?} sync log entries daemon running ...", this.me);

            let mut task_number = 0;
            while let Ok(event) = rx.recv() {
                if !this.keep_running.load(Ordering::Relaxed) {
                    break;
                }
                if !this.inner_state.lock().is_leader() {
                    continue;
                }
                for peer in this.peers.iter() {
                    let peer = *peer;
                    if event.should_schedule(peer) {
                        let progress = &peer_progress[peer.0];
                        if let Event::NewTerm(_term, index) = event {
                            progress.reset_progress(index);
                        }
                        // Only schedule a new task if the last task has cleared
                        // the queue of RPC requests.
                        if progress.should_schedule() {
                            task_number += 1;
                            this.thread_pool.spawn(Self::sync_log_entries(
                                this.inner_state.clone(),
                                this.sync_log_entries_comms.clone(),
                                progress.clone(),
                                this.apply_command_signal.clone(),
                                TaskNumber(task_number),
                            ));
                        }
                    }
                }
            }

            log::info!("{:?} sync log entries daemon done.", this.me);
        }
    }

    /// Syncs log entries to a peer once, requests a new sync if that fails.
    ///
    /// Sends an `AppendEntries` request if the planned next log entry to sync
    /// is after log start (and thus not covered by the log snapshot). Sends an
    /// `InstallSnapshot` request otherwise. The responses of those two types of
    /// requests are handled in a similar way.
    ///
    /// The peer might respond with
    /// * Success. Updates the internal record of how much log the peer holds.
    /// Marks new log entries as committed if we have a quorum of peers that
    /// have persisted the log entries. Note that we do not check if there are
    /// more items that can be sent to the peer. A new task will be scheduled
    /// for that outside of this daemon.
    ///
    /// * Nothing at all. A new request to sync log entries will be added to the
    /// `new_log_entry` queue.
    ///
    /// * The log has diverged. The peer disagrees with the request. We'll move
    /// the "planned next log entry to sync" towards the log start and request
    /// to sync again via the `new_log_entry` queue. The backoff will be
    /// exponential until it exceeds the log start, at which point the request
    /// becomes a `InstallSnapshot`. Note this case is impossible in a response
    /// to a `InstallSnapshot` RPC.
    ///
    /// * The log entry has been archived. The peer has taken a snapshot at that
    /// position and thus cannot verify the request. Along with this response,
    /// the peer sends back its commit index, which is guaranteed not to be
    /// shadowed by that snapshot. The sync will be retried at that commit index
    /// via the `new_log_entry` queue. Note the follow up sync could still fail
    /// for the same reason, as the peer might have moved its commit index.
    /// However syncing will eventually succeed, since the peer cannot move its
    /// commit index indefinitely without accepting any log sync requests from
    /// the leader.
    ///
    /// In the last two cases, the "planned next index to sync" can move towards
    /// the log start and end, respectively. We will not move back and forth in
    /// a mixed sequence of those two failures. The reasoning is that after a
    /// failure of the last case, we will never hit the other failure again,
    /// since in the last case we always sync log entry at a committed index,
    /// and a committed log entry can never diverge.
    async fn sync_log_entries(
        rf: Arc<Mutex<RaftState<Command>>>,
        comms: SyncLogEntriesComms,
        progress: PeerProgress,
        apply_command_signal: Arc<Condvar>,
        task_number: TaskNumber,
    ) {
        if !progress.take_task() {
            return;
        }

        let peer = progress.peer;
        let operation =
            Self::build_sync_log_entries(&rf, &progress, task_number);
        let (term, prev_log_index, match_index, succeeded) = match operation {
            SyncLogEntriesOperation::AppendEntries(args) => {
                let term = args.term;
                let prev_log_index = args.prev_log_index;
                let match_index = args.prev_log_index + args.entries.len();
                let succeeded = Self::append_entries(peer, args).await;

                (term, prev_log_index, match_index, succeeded)
            }
            SyncLogEntriesOperation::InstallSnapshot(args) => {
                let term = args.term;
                let prev_log_index = args.last_included_index;
                let match_index = args.last_included_index;
                let succeeded = Self::install_snapshot(peer, args).await;

                (term, prev_log_index, match_index, succeeded)
            }
            SyncLogEntriesOperation::None => return,
        };

        match succeeded {
            Ok(SyncLogEntriesResult::Success) => {
                let mut rf = rf.lock();

                if !rf.is_leader() {
                    return;
                }

                if rf.current_term != term {
                    return;
                }

                check_or_record!(
                    match_index < rf.log.end(),
                    ErrorKind::LeaderLogShrunk(match_index),
                    "The leader log shrunk",
                    &rf
                );

                progress.record_success(match_index);
                let peer_index = peer.0;
                if match_index > rf.match_index[peer_index] {
                    rf.match_index[peer_index] = match_index;
                    let mut matched = rf.match_index.to_vec();
                    let mid = matched.len() / 2 + 1;
                    matched.sort_unstable();
                    let new_commit_index = matched[mid];
                    if new_commit_index > rf.commit_index
                        && rf.log.at(new_commit_index).term == rf.current_term
                    {
                        log::info!(
                            "{:?} moving leader commit index to {} in {:?}",
                            rf.leader_id,
                            new_commit_index,
                            task_number
                        );
                        // COMMIT_INDEX_INVARIANT, SNAPSHOT_INDEX_INVARIANT:
                        // Index new_commit_index exists in the log array,
                        // which implies new_commit_index is in range
                        // [log.start(), log.end()).
                        rf.commit_index = new_commit_index;
                        apply_command_signal.notify_one();
                    }
                }
                // After each round of install snapshot, we must schedule
                // another round of append entries. The extra round must be run
                // even if match_index did not move after the snapshot is
                // installed.

                // For example,
                // 1. Leader committed index 10, received another request at
                //    index 11.
                // 2. Leader sends append entries to all peers.
                // 3. Leader commits index 11. At this time, append entries to
                //    peer X has not returned, while other append entries have.
                // 4. Leader snapshots index 11, received another request at
                //    index 12.
                // 5. Leader needs to update peer X, but does not have the
                //    commit at index 11 any more. Leader then sends install
                //    snapshot to peer X at index 11.
                // 6. The original append entries request to peer X returns
                //    successfully, moving match_index to 11.
                // 7. The install snapshot request returns successfully.
                //
                // The installed snapshot is at index 11, which is already sent
                // by the previous append entries request. Thus the match index
                // did not move. Still, we need the extra round of append
                // entries to peer X for log entry at index 12.
                if prev_log_index == match_index {
                    // If we did not make any progress this time, try again.
                    // This can only happen when installing snapshots.
                    comms.rerun(peer);
                }
            }
            Ok(SyncLogEntriesResult::Archived(committed)) => {
                let rf = rf.lock();

                check_or_record!(
                    prev_log_index < committed.index,
                    ErrorKind::RefusedSnapshotAfterCommitted(
                        prev_log_index,
                        committed.index
                    ),
                    format!(
                        "{:?} misbehaves: claimed log index {} is archived, \
                        but commit index is at {:?}) which is before that",
                        peer, prev_log_index, committed
                    ),
                    &rf
                );

                Self::check_committed(&rf, peer, &committed);

                // Next index moves towards the log end. This is the only place
                // where that happens. committed.index should be between log
                // start and end, guaranteed by check_committed() above.
                progress.record_success(committed.index + 1);

                comms.rerun(peer);
            }
            Ok(SyncLogEntriesResult::Diverged(committed)) => {
                let rf = rf.lock();
                check_or_record!(
                    prev_log_index > committed.index,
                    ErrorKind::DivergedBeforeCommitted(
                        prev_log_index,
                        committed.index
                    ),
                    format!(
                        "{:?} claimed log index {} does not match, \
                         but commit index is at {:?}) which is after that.",
                        peer, prev_log_index, committed
                    ),
                    &rf
                );
                Self::check_committed(&rf, peer, &committed);

                progress.record_failure(committed.index);

                comms.rerun(peer);
            }
            // Do nothing, not our term anymore.
            Ok(SyncLogEntriesResult::TermElapsed(term)) => {
                RemoteContext::<Command>::term_marker().mark(term);
            }
            Err(_) => {
                tokio::time::sleep(HEARTBEAT_INTERVAL).await;
                comms.rerun(peer);
            }
        };
    }

    fn check_committed(
        rf: &RaftState<Command>,
        peer: Peer,
        committed: &IndexTerm,
    ) {
        if committed.index < rf.log.start() {
            return;
        }
        check_or_record!(
            committed.index < rf.log.end(),
            ErrorKind::CommittedBeyondEnd(committed.index),
            format!(
                "Follower {:?} committed a log entry {:?} that is
                beyond the end of the leader log at {:?}",
                peer,
                committed,
                rf.log.end(),
            ),
            rf
        );
        let local_term = rf.log.at(committed.index).term;
        check_or_record!(
            committed.term == local_term,
            ErrorKind::DivergedAtCommitted(committed.index),
            format!(
                "{:?} committed log diverged at {:?}: {:?} v.s. leader {:?}",
                peer, committed.index, committed.term, local_term
            ),
            rf
        );
    }

    fn build_sync_log_entries(
        rf: &Mutex<RaftState<Command>>,
        progress: &PeerProgress,
        task_number: TaskNumber,
    ) -> SyncLogEntriesOperation<Command> {
        let rf = rf.lock();
        if !rf.is_leader() {
            return SyncLogEntriesOperation::None;
        }

        let peer = progress.peer;

        // To send AppendEntries request, next_index must be strictly larger
        // than start(). Otherwise we won't be able to know the log term of the
        // entry right before next_index.
        let next_index = progress.next_index();
        if next_index > rf.log.start() {
            if next_index < rf.log.end() {
                log::debug!(
                    "{:?} building append entries {:?} from {} to {:?}",
                    rf.leader_id,
                    task_number,
                    next_index - 1,
                    peer
                );
                SyncLogEntriesOperation::AppendEntries(
                    Self::build_append_entries(&rf, next_index),
                )
            } else {
                log::debug!(
                    "{:?} nothing in append entries {:?} to {:?}",
                    rf.leader_id,
                    task_number,
                    peer
                );
                SyncLogEntriesOperation::None
            }
        } else {
            log::debug!(
                "{:?} installing snapshot {:?} at {} to {:?}",
                rf.leader_id,
                task_number,
                rf.log.first_index_term().index,
                peer,
            );
            SyncLogEntriesOperation::InstallSnapshot(
                Self::build_install_snapshot(&rf),
            )
        }
    }

    fn build_append_entries(
        rf: &RaftState<Command>,
        next_index: Index,
    ) -> AppendEntriesArgs<Command> {
        // It is guaranteed that next_index <= rf.log.end(). Panic otherwise.
        let prev_log_index = next_index - 1;
        let prev_log_term = rf.log.at(prev_log_index).term;
        AppendEntriesArgs {
            term: rf.current_term,
            leader_id: rf.leader_id,
            prev_log_index,
            prev_log_term,
            entries: rf.log.after(next_index).to_vec(),
            leader_commit: rf.commit_index,
        }
    }

    const APPEND_ENTRIES_RETRY: usize = 1;
    async fn append_entries(
        peer: Peer,
        args: AppendEntriesArgs<Command>,
    ) -> std::io::Result<SyncLogEntriesResult> {
        let beat_ticker = RemoteContext::<Command>::beat_ticker(peer);
        let rpc_client = RemoteContext::<Command>::rpc_client(peer);

        let term = args.term;
        let beat = beat_ticker.next_beat();
        let reply = retry_rpc(
            Self::APPEND_ENTRIES_RETRY,
            RPC_DEADLINE,
            move |_round| rpc_client.append_entries(args.clone()),
        )
        .await?;
        Ok(if reply.term == term {
            beat_ticker.tick(beat);
            if let Some(committed) = reply.committed {
                if reply.success {
                    SyncLogEntriesResult::Archived(committed)
                } else {
                    SyncLogEntriesResult::Diverged(committed)
                }
            } else {
                SyncLogEntriesResult::Success
            }
        } else {
            SyncLogEntriesResult::TermElapsed(reply.term)
        })
    }

    fn build_install_snapshot(rf: &RaftState<Command>) -> InstallSnapshotArgs {
        let (last, snapshot) = rf.log.snapshot();
        InstallSnapshotArgs {
            term: rf.current_term,
            leader_id: rf.leader_id,
            last_included_index: last.index,
            last_included_term: last.term,
            data: snapshot.to_owned(),
            offset: 0,
            done: true,
        }
    }

    const INSTALL_SNAPSHOT_RETRY: usize = 1;
    async fn install_snapshot(
        peer: Peer,
        args: InstallSnapshotArgs,
    ) -> std::io::Result<SyncLogEntriesResult> {
        let beat_ticker = RemoteContext::<Command>::beat_ticker(peer);
        let rpc_client = RemoteContext::<Command>::rpc_client(peer);

        let term = args.term;
        let beat = beat_ticker.next_beat();
        let reply = retry_rpc(
            Self::INSTALL_SNAPSHOT_RETRY,
            RPC_DEADLINE,
            move |_round| rpc_client.install_snapshot(args.clone()),
        )
        .await?;
        Ok(if reply.term == term {
            beat_ticker.tick(beat);
            if let Some(committed) = reply.committed {
                SyncLogEntriesResult::Archived(committed)
            } else {
                SyncLogEntriesResult::Success
            }
        } else {
            SyncLogEntriesResult::TermElapsed(reply.term)
        })
    }
}

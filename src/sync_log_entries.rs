use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::check_or_record;
use crate::daemon_env::{Daemon, ErrorKind};
use crate::index_term::IndexTerm;
use crate::term_marker::TermMarker;
use crate::utils::{retry_rpc, SharedSender, RPC_DEADLINE};
use crate::{
    AppendEntriesArgs, InstallSnapshotArgs, Peer, Raft, RaftState, RemoteRaft,
    Term, HEARTBEAT_INTERVAL_MILLIS,
};

#[repr(align(64))]
struct Opening(Arc<AtomicUsize>);

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
// 4. default: a default value is used as the first element of log.
impl<Command> Raft<Command>
where
    Command: 'static + Clone + Send + serde::Serialize + Default,
{
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
    pub(crate) fn run_log_entry_daemon(&mut self) {
        let (tx, rx) = std::sync::mpsc::channel::<Option<Peer>>();
        let tx = SharedSender::new(tx);
        self.new_log_entry.replace(tx);

        // Clone everything that the thread needs.
        let this = self.clone();
        let join_handle = std::thread::spawn(move || {
            // Note: do not change this to `let _ = ...`.
            let _guard = this.daemon_env.for_scope();

            log::info!("{:?} sync log entries daemon running ...", this.me);

            let mut openings = vec![];
            openings.resize_with(this.peers.len(), || {
                Opening(Arc::new(AtomicUsize::new(0)))
            });
            let openings = openings; // Not mutable beyond this point.

            let mut task_number = 0;
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
                            task_number += 1;
                            this.thread_pool.spawn(Self::sync_log_entries(
                                this.inner_state.clone(),
                                rpc_client.clone(),
                                i,
                                this.new_log_entry.clone().unwrap(),
                                openings[i].0.clone(),
                                this.apply_command_signal.clone(),
                                this.term_marker(),
                                TaskNumber(task_number),
                            ));
                        }
                    }
                }
            }

            log::info!("{:?} sync log entries daemon done.", this.me);

            let stop_wait_group = this.stop_wait_group.clone();
            // Making sure the rest of `this` is dropped before the wait group.
            drop(this);
            drop(stop_wait_group);
        });
        self.daemon_env
            .watch_daemon(Daemon::SyncLogEntries, join_handle);
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
    #[allow(clippy::too_many_arguments)]
    async fn sync_log_entries(
        rf: Arc<Mutex<RaftState<Command>>>,
        rpc_client: impl RemoteRaft<Command>,
        peer_index: usize,
        rerun: SharedSender<Option<Peer>>,
        opening: Arc<AtomicUsize>,
        apply_command_signal: Arc<Condvar>,
        term_marker: TermMarker<Command>,
        task_number: TaskNumber,
    ) {
        if opening.swap(0, Ordering::SeqCst) == 0 {
            return;
        }

        let operation =
            Self::build_sync_log_entries(&rf, peer_index, task_number);
        let (term, prev_log_index, match_index, succeeded) = match operation {
            SyncLogEntriesOperation::AppendEntries(args) => {
                let term = args.term;
                let prev_log_index = args.prev_log_index;
                let match_index = args.prev_log_index + args.entries.len();
                let succeeded = Self::append_entries(&rpc_client, args).await;

                (term, prev_log_index, match_index, succeeded)
            }
            SyncLogEntriesOperation::InstallSnapshot(args) => {
                let term = args.term;
                let prev_log_index = args.last_included_index;
                let match_index = args.last_included_index;
                let succeeded = Self::install_snapshot(&rpc_client, args).await;

                (term, prev_log_index, match_index, succeeded)
            }
            SyncLogEntriesOperation::None => return,
        };

        let peer = Peer(peer_index);
        match succeeded {
            Ok(SyncLogEntriesResult::Success) => {
                let mut rf = rf.lock();

                check_or_record!(
                    match_index < rf.log.end(),
                    ErrorKind::LeaderLogShrunk(match_index),
                    "The leader log shrunk",
                    &rf
                );

                if !rf.is_leader() {
                    return;
                }

                if rf.current_term != term {
                    return;
                }

                rf.next_index[peer_index] = match_index + 1;
                rf.current_step[peer_index] = 0;
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
                    let _ = rerun.send(Some(Peer(peer_index)));
                }
            }
            Ok(SyncLogEntriesResult::Archived(committed)) => {
                let mut rf = rf.lock();

                check_or_record!(
                    prev_log_index < committed.index,
                    ErrorKind::RefusedSnapshotAfterCommitted(
                        prev_log_index,
                        committed.index
                    ),
                    format!(
                        "Peer {} misbehaves: claimed log index {} is archived, \
                        but commit index is at {:?}) which is before that",
                        peer_index, prev_log_index, committed
                    ),
                    &rf
                );

                Self::check_committed(&rf, peer, committed.clone());

                rf.current_step[peer_index] = 0;
                // Next index moves towards the log end. This is the only place
                // where that happens. committed.index should be between log
                // start and end, guaranteed by check_committed() above.
                rf.next_index[peer_index] = committed.index;

                // Ignore the error. The log syncing thread must have died.
                let _ = rerun.send(Some(Peer(peer_index)));
            }
            Ok(SyncLogEntriesResult::Diverged(committed)) => {
                let mut rf = rf.lock();
                check_or_record!(
                    prev_log_index > committed.index,
                    ErrorKind::DivergedBeforeCommitted(
                        prev_log_index,
                        committed.index
                    ),
                    format!(
                        "Peer {} claimed log index {} does not match, \
                         but commit index is at {:?}) which is after that.",
                        peer_index, prev_log_index, committed
                    ),
                    &rf
                );
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
            Ok(SyncLogEntriesResult::TermElapsed(term)) => {
                term_marker.mark(term);
            }
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
        peer_index: usize,
        task_number: TaskNumber,
    ) -> SyncLogEntriesOperation<Command> {
        let rf = rf.lock();
        if !rf.is_leader() {
            return SyncLogEntriesOperation::None;
        }

        // To send AppendEntries request, next_index must be strictly larger
        // than start(). Otherwise we won't be able to know the log term of the
        // entry right before next_index.
        if rf.next_index[peer_index] > rf.log.start() {
            log::debug!(
                "{:?} building append entries {:?} from {} to {:?}",
                rf.leader_id,
                task_number,
                rf.next_index[peer_index] - 1,
                Peer(peer_index)
            );
            SyncLogEntriesOperation::AppendEntries(Self::build_append_entries(
                &rf, peer_index,
            ))
        } else {
            log::debug!(
                "{:?} installing snapshot {:?} at {} to {:?}",
                rf.leader_id,
                task_number,
                rf.log.first_index_term().index,
                Peer(peer_index)
            );
            SyncLogEntriesOperation::InstallSnapshot(
                Self::build_install_snapshot(&rf),
            )
        }
    }

    fn build_append_entries(
        rf: &RaftState<Command>,
        peer_index: usize,
    ) -> AppendEntriesArgs<Command> {
        // It is guaranteed that next_index <= rf.log.end(). Panic otherwise.
        let prev_log_index = rf.next_index[peer_index] - 1;
        let prev_log_term = rf.log.at(prev_log_index).term;
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
        rpc_client: &dyn RemoteRaft<Command>,
        args: AppendEntriesArgs<Command>,
    ) -> std::io::Result<SyncLogEntriesResult> {
        let term = args.term;
        let reply = retry_rpc(
            Self::APPEND_ENTRIES_RETRY,
            RPC_DEADLINE,
            move |_round| rpc_client.append_entries(args.clone()),
        )
        .await?;
        Ok(if reply.term == term {
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
        rpc_client: &dyn RemoteRaft<Command>,
        args: InstallSnapshotArgs,
    ) -> std::io::Result<SyncLogEntriesResult> {
        let term = args.term;
        let reply = retry_rpc(
            Self::INSTALL_SNAPSHOT_RETRY,
            RPC_DEADLINE,
            move |_round| rpc_client.install_snapshot(args.clone()),
        )
        .await?;
        Ok(if reply.term == term {
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

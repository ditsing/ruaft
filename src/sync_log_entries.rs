use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::check_or_record;
use crate::daemon_env::ErrorKind;
use crate::index_term::IndexTerm;
use crate::utils::{retry_rpc, RPC_DEADLINE};
use crate::{
    AppendEntriesArgs, InstallSnapshotArgs, Peer, Raft, RaftState, RpcClient,
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
    pub(crate) fn run_log_entry_daemon(&mut self) {
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
                            this.thread_pool.spawn(Self::sync_log_entries(
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

    async fn sync_log_entries(
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

        let operation = Self::build_sync_log_entries(&rf, peer_index);
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
                            // COMMIT_INDEX_INVARIANT, SNAPSHOT_INDEX_INVARIANT:
                            // Index new_commit_index exists in the log array,
                            // which implies new_commit_index is in range
                            // [log.start(), log.end()).
                            rf.commit_index = new_commit_index;
                            apply_command_signal.notify_one();
                        }
                    }
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
                // where that happens.
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
            Ok(SyncLogEntriesResult::TermElapsed(_)) => {}
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
    ) -> SyncLogEntriesOperation<Command> {
        let rf = rf.lock();
        if !rf.is_leader() {
            return SyncLogEntriesOperation::None;
        }

        // To send AppendEntries request, next_index must be strictly larger
        // than start(). Otherwise we won't be able to know the log term of the
        // entry right before next_index.
        if rf.next_index[peer_index] > rf.log.start() {
            SyncLogEntriesOperation::AppendEntries(Self::build_append_entries(
                &rf, peer_index,
            ))
        } else {
            SyncLogEntriesOperation::InstallSnapshot(
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
    ) -> std::io::Result<SyncLogEntriesResult> {
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
        rpc_client: &RpcClient,
        args: InstallSnapshotArgs,
    ) -> std::io::Result<SyncLogEntriesResult> {
        let term = args.term;
        let reply = retry_rpc(
            Self::INSTALL_SNAPSHOT_RETRY,
            RPC_DEADLINE,
            move |_round| rpc_client.call_install_snapshot(args.clone()),
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

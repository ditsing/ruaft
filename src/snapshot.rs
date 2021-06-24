use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_utils::sync::{Parker, Unparker};
use parking_lot::{Condvar, Mutex};

use crate::check_or_record;
use crate::daemon_env::ErrorKind;
use crate::{Index, Raft};

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub last_included_index: Index,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SnapshotDaemon {
    unparker: Option<Unparker>,
    current_snapshot: Arc<(Mutex<Snapshot>, Condvar)>,
}

pub trait RequestSnapshotFnMut: 'static + Send + FnMut(Index) {}

impl<T: 'static + Send + FnMut(Index)> RequestSnapshotFnMut for T {}

impl SnapshotDaemon {
    pub(crate) fn save_snapshot(&self, snapshot: Snapshot) {
        let mut curr = self.current_snapshot.0.lock();
        if curr.last_included_index < snapshot.last_included_index {
            *curr = snapshot;
        }
        self.current_snapshot.1.notify_one();
    }

    pub(crate) fn trigger(&self) {
        match &self.unparker {
            Some(unparker) => unparker.unpark(),
            None => {}
        }
    }

    const MIN_SNAPSHOT_INDEX_INTERVAL: usize = 100;

    pub(crate) fn log_grow(&self, first_index: Index, last_index: Index) {
        if last_index - first_index > Self::MIN_SNAPSHOT_INDEX_INTERVAL {
            self.trigger();
        }
    }

    pub(crate) fn kill(&self) {
        self.trigger();
        // Acquire the lock to make sure the daemon thread either has been
        // waiting on the condition, or has not checked `keep_running` yet.
        let _ = self.current_snapshot.0.lock();
        self.current_snapshot.1.notify_all();
    }
}

impl<C: 'static + Clone + Default + Send + serde::Serialize> Raft<C> {
    pub fn save_snapshot(&self, snapshot: Snapshot) {
        self.snapshot_daemon.save_snapshot(snapshot)
    }

    pub(crate) fn run_snapshot_daemon(
        &mut self,
        max_state_size: Option<usize>,
        mut request_snapshot: impl RequestSnapshotFnMut,
    ) {
        let max_state_size = match max_state_size {
            Some(max_state_size) => max_state_size,
            None => return,
        };

        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        self.snapshot_daemon.unparker.replace(unparker.clone());

        let keep_running = self.keep_running.clone();
        let rf = self.inner_state.clone();
        let persister = self.persister.clone();
        let snapshot_daemon = self.snapshot_daemon.clone();
        let daemon_env = self.daemon_env.clone();
        let stop_wait_group = self.stop_wait_group.clone();

        let join_handle = std::thread::spawn(move || loop {
            // Note: do not change this to `let _ = ...`.
            let _guard = daemon_env.for_scope();

            parker.park();
            if !keep_running.load(Ordering::SeqCst) {
                // Explicitly drop every thing.
                drop(keep_running);
                drop(rf);
                drop(persister);
                drop(snapshot_daemon);
                drop(daemon_env);
                drop(stop_wait_group);
                break;
            }
            if persister.state_size() >= max_state_size {
                let log_start = rf.lock().log.first_index_term();
                let snapshot = {
                    let mut snapshot =
                        snapshot_daemon.current_snapshot.0.lock();
                    if keep_running.load(Ordering::SeqCst)
                        && snapshot.last_included_index <= log_start.index
                    {
                        request_snapshot(log_start.index + 1);
                        snapshot_daemon.current_snapshot.1.wait(&mut snapshot);
                    }
                    snapshot.clone()
                };

                let mut rf = rf.lock();
                if rf.log.first_index_term() != log_start {
                    // Another snapshot was installed, let's try again.
                    unparker.unpark();
                    continue;
                }
                if snapshot.last_included_index <= rf.log.start() {
                    // It seems the request_snapshot callback is misbehaving,
                    // let's try again.
                    unparker.unpark();
                    continue;
                }

                check_or_record!(
                    snapshot.last_included_index < rf.log.end(),
                    ErrorKind::SnapshotAfterLogEnd(
                        snapshot.last_included_index,
                    ),
                    "Snapshot contains data that is not in the log. \
                     This could happen when logs shrinks.",
                    &rf
                );

                rf.log.shift(snapshot.last_included_index, snapshot.data);
                persister.save_snapshot_and_state(
                    rf.persisted_state().into(),
                    rf.log.snapshot().1,
                );
            }
        });
        self.daemon_env.watch_daemon(join_handle);
    }
}

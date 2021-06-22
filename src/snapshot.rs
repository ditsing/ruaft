use std::sync::atomic::Ordering;

use crossbeam_utils::sync::{Parker, Unparker};

use crate::{Index, Raft};
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

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
        let stop_wait_group = self.stop_wait_group.clone();

        std::thread::spawn(move || loop {
            parker.park();
            if !keep_running.load(Ordering::SeqCst) {
                // Explicitly drop every thing.
                drop(keep_running);
                drop(rf);
                drop(persister);
                drop(snapshot_daemon);
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

                if snapshot.last_included_index >= rf.log.end() {
                    // We recently rolled back some of the committed logs. This
                    // can happen but usually the same exact log entries will be
                    // installed in the next AppendEntries request.
                    // There is no need to retry, because when the log entries
                    // are re-committed, we will be notified again.

                    // We will not be notified when the log length changes. Thus
                    // when the log length grows to passing last_included_index
                    // the first time, no snapshot will be taken, although
                    // nothing is preventing it to be done. We will wait until
                    // at least one more entry is committed.
                    continue;
                }

                rf.log.shift(snapshot.last_included_index, snapshot.data);
                persister.save_snapshot_and_state(
                    rf.persisted_state().into(),
                    rf.log.snapshot().1,
                );
            }
        });
    }
}

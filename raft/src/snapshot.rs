use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_utils::sync::{Parker, Unparker};
use parking_lot::{Condvar, Mutex};

use crate::check_or_record;
use crate::daemon_env::ErrorKind;
use crate::storage::RaftStorageMonitorTrait;
use crate::{Index, Raft};

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub last_included_index: Index,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct SnapshotDaemon {
    unparker: Option<Unparker>,
    current_snapshot: Arc<(Mutex<Snapshot>, Condvar)>,
}

pub trait RequestSnapshotFnMut: 'static + Send + FnMut(Index) {}

impl<T: 'static + Send + FnMut(Index)> RequestSnapshotFnMut for T {}

impl SnapshotDaemon {
    /// Create a new snapshot daemon.
    pub fn create() -> Self {
        Self {
            unparker: None,
            current_snapshot: Default::default(),
        }
    }

    /// Saves the snapshot into the staging area of the daemon, before it is
    /// applied to the log.
    ///
    /// Does nothing if the snapshot has a lower index than any snapshot before.
    pub(crate) fn save_snapshot(&self, snapshot: Snapshot) {
        let mut curr = self.current_snapshot.0.lock();
        // The new snapshot can have a last_included_index that is smaller than
        // the current snapshot, if this instance is a follower and the leader
        // has installed a new snapshot on it.
        // Note some stale snapshots might still fall through the crack, if the
        // current snapshot has been taken by the snapshot daemon thread,
        // leaving a default value in curr. In that scenario,
        // last_included_index is zero, and any snapshots are allowed.
        if curr.last_included_index < snapshot.last_included_index {
            *curr = snapshot;
        }
        self.current_snapshot.1.notify_one();
    }

    /// Wakes up the daemon and gives it a chance to request a new snapshot.
    pub(crate) fn trigger(&self) {
        match &self.unparker {
            Some(unparker) => unparker.unpark(),
            None => {}
        }
    }

    const MIN_SNAPSHOT_INDEX_INTERVAL: usize = 100;

    /// Notifies the daemon that the log has grown. It might be a good time to
    /// request a new snapshot.
    // We cannot simply deliver snapshot requests as one type of commands in the
    // apply_command daemon. A snapshot should be requested when
    // 1. a new log entry is applied to the state machine, or
    // 2. when the log grow out of the limit.
    //
    // The first scenario fits naturally into the duties of apply_command. The
    // second one, i.e. log_grow(), does not. The apply_command daemon does not
    // get notified when the log grows. Waking up the daemon when the log grow
    // can be costly.
    //
    // Another option is to allow other daemons sending messages to the state
    // machine. That would require ApplyCommandFunc to be shared by two threads.
    // Adding more requirements to external interface is also something we would
    // rather not do.
    pub(crate) fn log_grow(&self, first_index: Index, last_index: Index) {
        if last_index - first_index > Self::MIN_SNAPSHOT_INDEX_INTERVAL {
            self.trigger();
        }
    }

    /// Wakes up the daemon and asks it to shutdown. Does not wait for the
    /// daemon to fully exit. This method never fails or blocks forever.
    pub(crate) fn kill(&self) {
        self.trigger();
        // Acquire the lock to make sure the daemon thread either has been
        // waiting on the condition, or has not checked `keep_running` yet.
        let _guard = self.current_snapshot.0.lock();
        self.current_snapshot.1.notify_all();
    }
}

impl<C: 'static + Clone + Send + serde::Serialize> Raft<C> {
    /// Saves the snapshot into a staging area before it is applied to the log.
    ///
    /// Does nothing if the snapshot has a lower index than any snapshot before.
    ///
    /// This method Will not block forever. It contains minimal logic so that it
    /// is safe to run on an application thread. There is no guarantee that the
    /// saved snapshot will be applied to the internal log.
    pub fn save_snapshot(&self, snapshot: Snapshot) {
        self.snapshot_daemon.save_snapshot(snapshot)
    }

    /// Runs a daemon that requests and handles application snapshot.
    ///
    /// A snapshot must be taken when the size of the persisted log exceeds the
    /// limit specified by `max_state_size`. The daemon also attempts to take
    /// the snapshot when there are more than 100 entries in the log.
    ///
    /// A snapshot is requested by calling `request_snapshot`. The callback
    /// accepts a parameter that specifies the minimal log index the new
    /// snapshot must contain. The callback should not block. The callback could
    /// be called again when a snapshot is being prepared. The callback can be
    /// called multiple times with the same minimal log index.
    ///
    /// A new snapshot is delivered by calling [`Raft::save_snapshot`]. The new
    /// snapshot will be saved in a temporary space. This daemon will wake up,
    /// apply the snapshot into the log and discard log entries before the
    /// snapshot. There is no guarantee that the snapshot will be applied.
    pub(crate) fn run_snapshot_daemon<T: RaftStorageMonitorTrait>(
        &mut self,
        storage_monitor: Option<T>,
        mut request_snapshot: impl RequestSnapshotFnMut,
    ) -> impl FnOnce() {
        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        self.snapshot_daemon.unparker.replace(unparker.clone());

        let keep_running = self.keep_running.clone();
        let me = self.me;
        let rf = self.inner_state.clone();
        let persister = self.persister.clone();
        let snapshot_daemon = self.snapshot_daemon.clone();

        log::info!("{:?} snapshot daemon running ...", me);
        let snapshot_daemon = move |storage_monitor: T| loop {
            parker.park();
            if !keep_running.load(Ordering::Acquire) {
                log::info!("{:?} snapshot daemon done.", me);

                // Explicitly drop every thing.
                drop(keep_running);
                drop(rf);
                drop(storage_monitor);
                drop(persister);
                drop(snapshot_daemon);
                break;
            }
            if storage_monitor.should_compact_log_now() {
                let log_start = rf.lock().log.first_index_term();
                let snapshot = {
                    let mut snapshot =
                        snapshot_daemon.current_snapshot.0.lock();
                    if keep_running.load(Ordering::Acquire)
                        && snapshot.last_included_index <= log_start.index
                    {
                        request_snapshot(log_start.index + 1);
                        snapshot_daemon.current_snapshot.1.wait(&mut snapshot);
                    }
                    // Take the snapshot and set the index to zero in its place.
                    // This defeats the stale snapshot protection implemented in
                    // `save_snapshot()`. Stale snapshots are tolerated below.
                    use std::ops::DerefMut;
                    std::mem::take(snapshot.deref_mut())
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
                check_or_record!(
                    snapshot.last_included_index <= rf.commit_index,
                    ErrorKind::SnapshotNotCommitted(
                        snapshot.last_included_index
                    ),
                    "Snapshot contains data that is not committed. \
                     This should never happen.",
                    &rf
                );

                // SNAPSHOT_INDEX_INVARIANT: log.start() is shifted to
                // last_included_index. We checked that last_included_index is
                // smaller than commit_index. This is the only place where
                // log.start() changes.
                rf.log.shift(snapshot.last_included_index, snapshot.data);
                let (index_term, snapshot) = rf.log.snapshot();
                persister.update_snapshot(index_term, snapshot);
            }
        };
        move || {
            if let Some(storage_monitor) = storage_monitor {
                snapshot_daemon(storage_monitor)
            }
        }
    }
}

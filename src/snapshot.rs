use crate::{Index, Raft};
use crossbeam_utils::sync::{Parker, Unparker};
use std::sync::atomic::Ordering;

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub last_included_index: Index,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SnapshotDaemon {
    unparker: Option<Unparker>,
}

pub trait RequestSnapshotFnMut:
    'static + Send + FnMut(Index) -> Snapshot
{
}

impl<T: 'static + Send + FnMut(Index) -> Snapshot> RequestSnapshotFnMut for T {}

impl SnapshotDaemon {
    pub(crate) fn trigger(&self) {
        match &self.unparker {
            Some(unparker) => unparker.unpark(),
            None => {}
        }
    }
}

impl<C: 'static + Clone + Default + Send + serde::Serialize> Raft<C> {
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
        let stop_wait_group = self.stop_wait_group.clone();

        std::thread::spawn(move || loop {
            parker.park();
            if !keep_running.load(Ordering::SeqCst) {
                // Explicitly drop every thing.
                drop(keep_running);
                drop(rf);
                drop(persister);
                drop(stop_wait_group);
                break;
            }
            if persister.state_size() >= max_state_size {
                let log_start = rf.lock().log.first_index_term();
                let snapshot = request_snapshot(log_start.index + 1);

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

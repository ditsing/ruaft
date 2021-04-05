use crate::{Index, Raft};
use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Snapshot {
    pub last_included_index: Index,
    pub data: Vec<u8>,
}

#[derive(Debug, Default)]
pub(crate) struct SnapshotDaemon {
    count: AtomicUsize,
    mutex: Mutex<usize>,
    cond: Condvar,
}

impl SnapshotDaemon {
    pub(crate) fn trigger_snapshot_soft(&self) -> usize {
        self.count.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn trigger_snapshot(&self) -> usize {
        let prev = *self.mutex.lock();
        let curr = self.trigger_snapshot_soft();
        self.cond.notify_one();
        assert!(curr >= prev);
        assert_ne!(curr, usize::MAX, "The counter overflowed.");
        curr
    }

    fn wait_for_triggering(&self) -> (usize, usize) {
        let mut guard = self.mutex.lock();
        loop {
            let curr = self.count.load(Ordering::SeqCst);
            if curr != *guard {
                let prev = *guard;
                *guard = curr;
                return (prev, curr);
            }
            self.cond.wait(&mut guard);
        }
    }
}

impl<C: 'static + Clone + Default + Send + serde::Serialize> Raft<C> {
    pub(crate) fn run_snapshot_daemon<Func>(
        &self,
        max_state_size: Option<usize>,
        mut request_snapshot: Func,
    ) where
        Func: 'static + Send + FnMut(Index) -> Snapshot,
    {
        let max_state_size = match max_state_size {
            Some(max_state_size) => max_state_size,
            None => return,
        };

        let rf = self.inner_state.clone();
        let snapshot_daemon = self.snapshot_daemon.clone();
        let persister = self.persister.clone();

        std::thread::spawn(move || loop {
            snapshot_daemon.wait_for_triggering();
            if persister.state_size() >= max_state_size {
                let (term, log_start) = {
                    let rf = rf.lock();
                    (rf.current_term, rf.log.first_index_term())
                };
                let snapshot = request_snapshot(log_start.index + 1);

                let mut rf = rf.lock();
                if rf.current_term != term
                    || rf.log.first_index_term() != log_start
                {
                    // Term has changed, or another snapshot was installed.
                    snapshot_daemon.trigger_snapshot_soft();
                    continue;
                }
                if snapshot.last_included_index <= rf.log.start()
                    || snapshot.last_included_index >= rf.log.end()
                {
                    // TODO(ditsing): Something happened.
                    snapshot_daemon.trigger_snapshot_soft();
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

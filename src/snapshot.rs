use crate::{Index, Raft};
use crossbeam_utils::sync::{Parker, Unparker};

pub struct Snapshot {
    pub last_included_index: Index,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SnapshotDaemon {
    unparker: Option<Unparker>,
}

impl SnapshotDaemon {
    pub(crate) fn trigger(&self) {
        match &self.unparker {
            Some(unparker) => unparker.unpark(),
            None => {}
        }
    }
}

impl<C: 'static + Clone + Default + Send + serde::Serialize> Raft<C> {
    pub(crate) fn run_snapshot_daemon<Func>(
        &mut self,
        max_state_size: Option<usize>,
        mut request_snapshot: Func,
    ) where
        Func: 'static + Send + FnMut(Index) -> Snapshot,
    {
        let max_state_size = match max_state_size {
            Some(max_state_size) => max_state_size,
            None => return,
        };

        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        self.snapshot_daemon.unparker.replace(unparker.clone());

        let rf = self.inner_state.clone();
        let persister = self.persister.clone();

        std::thread::spawn(move || loop {
            parker.park();
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
                    unparker.unpark();
                    continue;
                }
                if snapshot.last_included_index <= rf.log.start()
                    || snapshot.last_included_index >= rf.log.end()
                {
                    // TODO(ditsing): Something happened.
                    unparker.unpark();
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

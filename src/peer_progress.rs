use std::sync::Arc;

use parking_lot::Mutex;

use crate::{Index, Peer};

struct PeerProgressInternal {
    next_index: Index,
    current_step: i64,
}

#[derive(Clone)]
pub(crate) struct PeerProgress {
    pub peer: Peer,
    internal: Arc<Mutex<PeerProgressInternal>>,
}

impl PeerProgress {
    pub fn create(peer_index: usize) -> Self {
        Self {
            peer: Peer(peer_index),
            internal: Arc::new(Mutex::new(PeerProgressInternal {
                next_index: 1,
                current_step: 0,
            })),
        }
    }

    pub fn reset_progress(&self, next_index: Index) {
        let mut internal = self.internal.lock();
        internal.next_index = next_index;
        internal.current_step = 0;
    }

    pub fn record_failure(&self, committed_index: Index) {
        let mut internal = self.internal.lock();
        let step = &mut internal.current_step;
        if *step < 5 {
            *step += 1;
        }
        let diff = 4 << *step;

        let next_index = &mut internal.next_index;
        if diff >= *next_index {
            *next_index = 1usize;
        } else {
            *next_index -= diff;
        }

        if *next_index < committed_index {
            *next_index = committed_index;
        }
    }

    pub fn record_success(&self, match_index: Index) {
        let mut internal = self.internal.lock();
        internal.next_index = match_index + 1;
        internal.current_step = 0;
    }

    pub fn next_index(&self) -> Index {
        self.internal.lock().next_index
    }
}

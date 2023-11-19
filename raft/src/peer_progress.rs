use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::{Index, Peer};

struct SharedIndexes {
    next_index: Index,
    current_step: i64,
}

struct SharedProgress {
    opening: AtomicUsize,
    indexes: Mutex<SharedIndexes>,
}

/// Progress a peer during log sync.
///
/// The number of unclaimed sync requests is stored here. This struct also
/// contains the last known of the length of the logs held by a peer.
#[derive(Clone)]
#[repr(align(64))]
pub(crate) struct PeerProgress {
    pub peer: Peer,
    internal: Arc<SharedProgress>,
}

impl PeerProgress {
    pub fn create(peer_index: usize) -> Self {
        Self {
            peer: Peer(peer_index),
            internal: Arc::new(SharedProgress {
                opening: AtomicUsize::new(0),
                indexes: Mutex::new(SharedIndexes {
                    next_index: 1,
                    current_step: 0,
                }),
            }),
        }
    }

    /// Claim all pending sync requests and clear the request counter.
    pub fn take_task(&self) -> bool {
        self.internal.opening.swap(0, Ordering::AcqRel) != 0
    }

    /// Increase the number of sync requests by one. A new round of log syncing
    /// should be started if this is the first of a group of requests. No new
    /// round is needed if the previous requests have not been claimed.
    pub fn should_schedule(&self) -> bool {
        self.internal.opening.fetch_add(1, Ordering::AcqRel) == 0
    }

    /// Reset progress data stored for a peer when a new term is started.
    pub fn reset_progress(&self, next_index: Index) {
        let mut internal = self.internal.indexes.lock();
        internal.next_index = next_index;
        internal.current_step = 0;
    }

    /// Record failure of serving a sync request. The failure is usually caused
    /// by peers disagreeing on the base commit. Here we chose to step back
    /// exponentially to limit the rounds of syncing required.
    pub fn record_failure(&self, committed_index: Index) {
        let mut internal = self.internal.indexes.lock();
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

    /// Record a success in serving a sync request. Move the pointer to the next
    /// log item to sync.
    pub fn record_success(&self, match_index: Index) {
        let mut internal = self.internal.indexes.lock();
        internal.next_index = match_index + 1;
        internal.current_step = 0;
    }

    /// Returns the next log index to sync.
    pub fn next_index(&self) -> Index {
        self.internal.indexes.lock().next_index
    }
}

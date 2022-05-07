#![allow(unused)]
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A beat is one request sent to a peer with a success response.
/// The `usize` within is the unique ID of the request.
#[derive(Debug, Eq, Ord, PartialOrd, PartialEq)]
pub(crate) struct Beat(usize);

/// A `BeatTicker` issues unique request IDs and records successful runs.
///
/// Each peer should have its own `BeatTicker`. Requests are ordered by the wall
/// time they call `BeatTicker::next_beat()`. Each successful request marks the
/// recognition from the peer that the sender (this instance) is the leader.
///
/// The leader status is continuous for a certain term. Imagine the following
/// scenario. We are elected leader at absolute time `X`, and send a message
/// to a peer at time `Y` (`Y > X`). The peer confirms the leader status by
/// replying to the message. We can then assume that the peer recognizes us as
/// the leader in entire time interval `[X, Y]`.
///
/// For each term, the starting point of the interval (`X`) is fixed. Newer
/// requests extends the interval further. Thus, we only need to record the last
/// confirmation from a peer.
///
/// At any time `T` that has `T > X`, if there are more than `N/2` peers
/// confirmed the leader status after `T`, we can be sure that at time `T` we
/// were the leader.
pub(crate) struct BeatTicker {
    // Beat count might overflow after 25 days at 2000 QPS to a single peer, if
    // we assume usize is 32-bit.
    // This should not be a problem because:
    // 1. usize is usually 64-bit.
    // 2. 2000 QPS is an overestimate.
    beat_count: AtomicUsize,
    ticked: AtomicUsize,
}

impl BeatTicker {
    /// Creates a `BeatTicker`.
    /// The first unique request ID issued by the ticker will be 1. The initial
    /// value of successful request will start at ID 0.
    fn create() -> Self {
        Self {
            beat_count: AtomicUsize::new(1),
            ticked: AtomicUsize::new(0),
        }
    }

    /// Issues the next unique request ID.
    pub fn next_beat(&self) -> Beat {
        let count = self.beat_count.fetch_add(1, Ordering::AcqRel);
        assert_ne!(count, usize::MAX, "BeatTicker count overflow");
        Beat(count)
    }

    /// Returns the newest beat (request ID).
    pub fn current_beat(&self) -> Beat {
        Beat(self.beat_count.load(Ordering::Acquire))
    }

    /// Marks a beat (request) as successful.
    pub fn tick(&self, beat: Beat) {
        self.ticked.fetch_max(beat.0, Ordering::AcqRel);
    }

    /// Returns the last successful beat (request ID).
    pub fn ticked(&self) -> Beat {
        Beat(self.ticked.load(Ordering::Acquire))
    }
}

/// A smart pointer to share `BeatTicker` among threads and tasks.
#[derive(Clone)]
pub(crate) struct SharedBeatTicker(Arc<BeatTicker>);

impl SharedBeatTicker {
    pub fn create() -> Self {
        Self(Arc::new(BeatTicker::create()))
    }
}

impl Deref for SharedBeatTicker {
    type Target = BeatTicker;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

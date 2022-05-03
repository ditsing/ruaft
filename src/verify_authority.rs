use crate::beat_ticker::{Beat, SharedBeatTicker};
use crate::daemon_env::Daemon;
use crate::{Raft, Term, HEARTBEAT_INTERVAL_MILLIS};
use crossbeam_utils::sync::{Parker, Unparker};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The result returned to a verify authority request.
/// This request is not directly exposed to end users. Instead it is used
/// internally to implement no-commit read-only requests.
pub(crate) enum VerifyAuthorityResult {
    Success,
    TermElapsed,
    TimedOut,
}

/// Token stored in the internal queue for authority verification. Each token
/// represents one verification request.
struct VerifyAuthorityToken {
    beats_moment: Vec<Beat>,
    rough_time: Instant,
    sender: tokio::sync::oneshot::Sender<VerifyAuthorityResult>,
}

#[derive(Clone, Copy, Default, Eq, Ord, PartialOrd, PartialEq)]
struct QueueIndex(usize);

/// The state of this daemon, should bee protected by a mutex.
struct VerifyAuthorityState {
    /// The current term. Might be behind the real term in the cluster.
    term: Term,
    /// Pending requests to verify authority.
    queue: VecDeque<VerifyAuthorityToken>,
    /// Number of requests that have been processed.
    start: QueueIndex,
    /// A vector of queue indexes. Each element in this vector indicates the
    /// index of the first request that has not been confirmed by the
    /// corresponding peer.
    /// These indexes include all processed requests. They will never go down.
    covered: Vec<QueueIndex>,
}

impl VerifyAuthorityState {
    pub fn create(peer_count: usize) -> Self {
        VerifyAuthorityState {
            term: Term(0),
            queue: Default::default(),
            start: QueueIndex(0),
            covered: vec![QueueIndex(0); peer_count],
        }
    }

    pub fn reset(&mut self, term: Term) {
        self.clear_tickets();

        self.term = term;
        self.start = QueueIndex(0);
        for item in self.covered.iter_mut() {
            *item = QueueIndex(0)
        }
    }

    pub fn clear_tickets(&mut self) {
        for token in self.queue.drain(..) {
            let _ = token.sender.send(VerifyAuthorityResult::TermElapsed);
        }
    }
}

#[derive(Clone)]
pub(crate) struct VerifyAuthorityDaemon {
    state: Arc<Mutex<VerifyAuthorityState>>,
    beat_tickers: Vec<SharedBeatTicker>,
    unparker: Option<Unparker>,
}

impl VerifyAuthorityDaemon {
    pub fn create(peer_count: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(VerifyAuthorityState::create(
                peer_count,
            ))),
            beat_tickers: (0..peer_count)
                .map(|_| SharedBeatTicker::create())
                .collect(),
            unparker: None,
        }
    }

    pub fn reset_state(&self, term: Term) {
        self.state.lock().reset(term);
        // Increase all beats by one to make sure upcoming verify authority
        // requests wait for beats in the current term. This in fact creates
        // phantom beats that will never be marked as completed by themselves.
        // They will be automatically `ticked()` when newer (real) beats are
        // created, sent and `ticked()`.
        for beat_ticker in self.beat_tickers.iter() {
            beat_ticker.next_beat();
        }
    }

    /// Enqueues a verify authority request. Returns a receiver of the
    /// verification result. Returns None if the term has passed.
    pub fn verify_authority_async(
        &self,
        current_term: Term,
    ) -> Option<tokio::sync::oneshot::Receiver<VerifyAuthorityResult>> {
        // The inflight beats are sent at least for `current_term`. This is
        // guaranteed by the fact that we immediately increase beats for all
        // peers after being elected. It further guarantees that the newest
        // beats we get here are at least as new as the phantom beats created by
        // `Self::reset_state()`.
        let beats_moment = self
            .beat_tickers
            .iter()
            .map(|beat_ticker| beat_ticker.current_beat())
            .collect();

        // The inflight beats could also be for any term after `current_term`.
        // We must check if the term stored in the daemon is the same as
        // `current_term`.
        // `state.term` could be smaller than `current_term`, if a new term is
        // started by someone else and we lost leadership.
        // `state.term` could be greater than `current_term`, if we lost
        // leadership but are elected leader again in a following term.
        // In both cases, we cannot confirm the leadership at `current_term`.
        let mut state = self.state.lock();
        if state.term != current_term {
            return None;
        }

        let (sender, receiver) = tokio::sync::oneshot::channel();
        let token = VerifyAuthorityToken {
            beats_moment,
            rough_time: Instant::now(),
            sender,
        };
        state.queue.push_back(token);

        Some(receiver)
    }

    /// Run one iteration of the verify authority daemon.
    /// Fetches the newest successful RPC response from peers, and mark verify
    /// authority requests as complete if they are covered by more than half of
    /// the replicas.
    pub fn run_verify_authority_iteration(&self) {
        // Opportunistic check: do nothing if we don't have any requests.
        if self.state.lock().queue.is_empty() {
            return;
        }

        for (peer_index, beat_ticker) in self.beat_tickers.iter().enumerate() {
            // Fetches the newest successful RPC response from the current peer.
            let ticked = beat_ticker.ticked();
            let mut state = self.state.lock();
            // Update progress with `ticked`. All requests that came before
            // `ticked` now have one more votes of leader authority from the
            // current peer.
            let first_not_ticked_index = state.queue.partition_point(|token| {
                token.beats_moment[peer_index] <= ticked
            });
            assert!(first_not_ticked_index >= state.covered[peer_index].0);
            state.covered[peer_index].0 = first_not_ticked_index;

            // Count the requests that has more than N / 2 votes. We always have
            // the vote from ourselves, but the value is 0 in `covered` array.
            let mut sorted_covered = state.covered.to_owned();
            sorted_covered.sort_unstable();
            let mid = sorted_covered.len() / 2 + 1;
            let new_start = sorted_covered[mid];

            // All requests before `new_start` is now verified.
            let verified = new_start.0 - state.start.0;
            for token in state.queue.drain(..verified) {
                for (index, beat) in token.beats_moment.iter().enumerate() {
                    assert!(self.beat_tickers[index].ticked() >= *beat);
                }
                let _ = token.sender.send(VerifyAuthorityResult::Success);
            }
            // Move the queue starting point.
            state.start = new_start;
        }
    }

    const VERIFY_AUTHORITY_REQUEST_EXPIRATION: Duration =
        Duration::from_millis(HEARTBEAT_INTERVAL_MILLIS * 2);

    /// Remove expired requests if we are no longer the leader.
    /// If we have lost leadership, we are unlikely to receive confirmations
    /// of past leadership state from peers. Requests are expired after two
    /// heartbeat period have passed. We do not immediately cancel all incoming
    /// requests, in hope that we could still answer them accurately without
    /// breaking the consistency guarantee.
    fn removed_expired_requests(&self, current_term: Term) {
        let mut state = self.state.lock();
        // Return if we are still the leader, or we become the leader again.
        //
        // Note that we do not hold the main raft state lock, thus the value of
        // `current_term` might not be up-to-date. We only update `state.term`
        // after an election. If in a term after `current_term`, we are elected
        // leader again, `state.term` could be updated and thus greater than the
        // (now stale) `current_term`. In that case, the queue should have been
        // reset. There will be no expired request to remove.
        if state.term >= current_term {
            return;
        }

        let expiring_line =
            Instant::now() - Self::VERIFY_AUTHORITY_REQUEST_EXPIRATION;
        // Assuming bounded clock skew, otherwise we will lose efficiency.
        let expired =
            |head: &VerifyAuthorityToken| head.rough_time < expiring_line;
        // Note rough_time might not be in increasing order, so we might still
        // have requests that are expired in the queue after the sweep.
        while state.queue.front().map_or(false, expired) {
            state
                .queue
                .pop_front()
                .map(|head| head.sender.send(VerifyAuthorityResult::TimedOut));
        }
    }

    pub fn kill(&self) {
        if let Some(unparker) = self.unparker.as_ref() {
            unparker.unpark();
        }
    }
}

impl<Command: 'static + Send> Raft<Command> {
    const BEAT_RECORDING_MAX_PAUSE: Duration = Duration::from_millis(20);

    /// Create a thread and runs the verify authority daemon.
    pub(crate) fn run_verify_authority_daemon(&mut self) {
        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        self.verify_authority_daemon.unparker.replace(unparker);

        let keep_running = self.keep_running.clone();
        let this_daemon = self.verify_authority_daemon.clone();
        let rf = self.inner_state.clone();

        let join_handle = std::thread::spawn(move || {
            while keep_running.load(Ordering::Acquire) {
                parker.park_timeout(Self::BEAT_RECORDING_MAX_PAUSE);
                this_daemon.run_verify_authority_iteration();
                let current_term = rf.lock().current_term;
                this_daemon.removed_expired_requests(current_term);
            }
        });
        self.daemon_env
            .watch_daemon(Daemon::VerifyAuthority, join_handle);
    }

    /// Create a verify authority request. Returns None if we are not the
    /// leader.
    #[allow(dead_code)]
    pub(crate) fn verify_authority_async(
        &self,
    ) -> Option<impl Future<Output = VerifyAuthorityResult>> {
        let term = {
            let rf = self.inner_state.lock();
            if !rf.is_leader() {
                return None;
            }

            rf.current_term
        };
        let receiver =
            self.verify_authority_daemon.verify_authority_async(term);
        receiver.map(|receiver| async move {
            receiver
                .await
                .expect("Verify authority daemon never drops senders")
        })
    }
}

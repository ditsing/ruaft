use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::beat_ticker::{Beat, SharedBeatTicker};
use crate::heartbeats::HEARTBEAT_INTERVAL;
use crate::{Index, Raft, Term};

/// The result returned to a verify authority request.
/// This request is not directly exposed to end users. Instead it is used
/// internally to implement no-commit read-only requests.
#[derive(Debug, Eq, PartialEq)]
pub enum VerifyAuthorityResult {
    Success(Index),
    TermElapsed,
    TimedOut,
}

/// Token stored in the internal queue for authority verification. Each token
/// represents one verification request.
#[derive(Debug)]
struct VerifyAuthorityToken {
    commit_index: Index,
    beats_moment: Vec<Beat>,
    rough_time: Instant,
    sender: tokio::sync::oneshot::Sender<VerifyAuthorityResult>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialOrd, PartialEq)]
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
    /// The index of the first commit, created at the start of the term.
    sentinel_commit_index: Index,
}

impl VerifyAuthorityState {
    pub fn create(peer_count: usize) -> Self {
        VerifyAuthorityState {
            term: Term(0),
            queue: VecDeque::new(),
            start: QueueIndex(0),
            covered: vec![QueueIndex(0); peer_count],
            sentinel_commit_index: 0,
        }
    }

    pub fn reset(&mut self, term: Term, sentinel_commit_index: Index) {
        self.clear_tickets();

        self.term = term;
        self.start = QueueIndex(0);
        self.sentinel_commit_index = sentinel_commit_index;
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
pub(crate) struct DaemonBeatTicker {
    beat_ticker: SharedBeatTicker,
    condvar: Arc<Condvar>,
}

impl DaemonBeatTicker {
    pub fn next_beat(&self) -> Beat {
        self.beat_ticker.next_beat()
    }

    pub fn tick(&self, beat: Beat) {
        self.beat_ticker.tick(beat);
        self.condvar.notify_one();
    }
}

#[derive(Clone)]
pub(crate) struct VerifyAuthorityDaemon {
    state: Arc<Mutex<VerifyAuthorityState>>,
    beat_tickers: Vec<SharedBeatTicker>,
    condvar: Arc<Condvar>,
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
            condvar: Arc::new(Condvar::new()),
        }
    }

    pub fn wait_for(&self, timeout: Duration) {
        let mut guard = self.state.lock();
        self.condvar.wait_for(&mut guard, timeout);
    }

    pub fn reset_state(&self, term: Term, sentinel_commit_index: Index) {
        self.state.lock().reset(term, sentinel_commit_index);
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
        commit_index: Index,
    ) -> Option<tokio::sync::oneshot::Receiver<VerifyAuthorityResult>> {
        let mut state = self.state.lock();
        // The inflight beats are sent at least for `current_term`. This is
        // guaranteed by the fact that we immediately increase beats for all
        // peers after being elected, before releasing the "elected" message to
        // the rest of the Raft system. The newest beats we get here are at
        // least as new as the phantom beats created by `Self::reset_state()`.
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
        if state.term != current_term {
            return None;
        }

        let (sender, receiver) = tokio::sync::oneshot::channel();
        let token = VerifyAuthorityToken {
            commit_index,
            beats_moment,
            rough_time: Instant::now(),
            sender,
        };
        state.queue.push_back(token);

        Some(receiver)
    }

    /// Run one iteration of the verify authority daemon.
    pub fn run_verify_authority_iteration(
        &self,
        current_term: Term,
        commit_index: Index,
    ) {
        // Opportunistic check: do nothing if we don't have any requests.
        if self.state.lock().queue.is_empty() {
            return;
        }

        self.clear_ticked_requests(commit_index);
        self.remove_expired_requests(current_term);
    }

    /// Fetches the newest successful RPC response from peers, and mark verify
    /// authority requests as complete if they are covered by more than half of
    /// the replicas.
    fn clear_ticked_requests(&self, commit_index: Index) {
        // Do not use ticks to clear requests if we have not committed at least
        // one log entry since the start of the term. At the start of the term,
        // the leader might not know the commit index of the previous leader.
        // This holds true even it is guaranteed that all entries committed by
        // the previous leader will be committed by the current leader.
        //
        // Similarly, if the sentinel is not committed, the leader cannot know
        // if all entries of the previous leader will be committed, in case the
        // leadership is lost before any commits can be made. Thus, the leader
        // cannot answer any queries before the sentinel is committed.
        if commit_index < self.state.lock().sentinel_commit_index {
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
            let new_covered = first_not_ticked_index + state.start.0;
            assert!(new_covered >= state.covered[peer_index].0);
            state.covered[peer_index].0 = new_covered;

            // Count the requests that has more than N / 2 votes. We always have
            // the vote from ourselves, but the value is 0 in `covered` array.
            let mut sorted_covered = state.covered.to_owned();
            sorted_covered.sort_unstable();
            let mid = sorted_covered.len() / 2 + 1;
            let new_start = sorted_covered[mid];

            // `state.start` could have been moved by other means, e.g. by a
            // subsequent commit of the same term after the beat is issued.
            // Then the relevant verify authority requests have been processed.
            // If all ticked requests have been processed, nothing needs to be
            // done. Skip to the next iteration.
            if new_start <= state.start {
                continue;
            }

            // All requests before `new_start` is now verified.
            let verified = new_start.0 - state.start.0;
            let sentinel_commit_index = state.sentinel_commit_index;
            for token in state.queue.drain(..verified) {
                let mut cnt = 0;
                for (index, beat) in token.beats_moment.iter().enumerate() {
                    if self.beat_tickers[index].ticked() >= *beat {
                        cnt += 1;
                    }
                }
                assert!(cnt + cnt + 1 >= self.beat_tickers.len());

                // Never verify authority before the sentinel commit index. The
                // previous leader might have exposed data up to the commit
                // right before the sentinel.
                let allowed_index =
                    if sentinel_commit_index > token.commit_index {
                        // sentinel_commit_index cannot be at 0 after the `if`.
                        sentinel_commit_index - 1
                    } else {
                        token.commit_index
                    };

                let _ = token
                    .sender
                    .send(VerifyAuthorityResult::Success(allowed_index));
            }
            // Move the queue starting point.
            state.start = new_start;
        }
    }

    const VERIFY_AUTHORITY_REQUEST_EXPIRATION: Duration =
        Duration::from_millis(HEARTBEAT_INTERVAL.as_millis() as u64 * 2);

    /// Remove expired requests if we are no longer the leader.
    /// If we have lost leadership, we are unlikely to receive confirmations
    /// of past leadership state from peers. Requests are expired after two
    /// heartbeat period have passed. We do not immediately cancel all incoming
    /// requests, in hope that we could still answer them accurately without
    /// breaking the consistency guarantee.
    fn remove_expired_requests(&self, current_term: Term) {
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
            state.start.0 += 1;
        }
    }

    pub fn beat_ticker(&self, peer_index: usize) -> DaemonBeatTicker {
        DaemonBeatTicker {
            beat_ticker: self.beat_tickers[peer_index].clone(),
            condvar: self.condvar.clone(),
        }
    }

    pub fn kill(&self) {
        let term = self.state.lock().term;
        // Fail all inflight verify authority requests. It is important to do
        // this so that the RPC framework could drop requests served by us and
        // release all references to the Raft instance.
        self.reset_state(term, Index::MAX);
        self.condvar.notify_all();
    }
}

impl<Command: 'static + Send> Raft<Command> {
    const BEAT_RECORDING_MAX_PAUSE: Duration = Duration::from_millis(20);

    /// Create a thread and runs the verify authority daemon.
    pub(crate) fn run_verify_authority_daemon(&self) -> impl FnOnce() {
        let me = self.me;
        let keep_running = self.keep_running.clone();
        let this_daemon = self.verify_authority_daemon.clone();
        let rf = self.inner_state.clone();

        move || {
            log::info!("{:?} verify authority daemon running ...", me);
            while keep_running.load(Ordering::Relaxed) {
                this_daemon.wait_for(Self::BEAT_RECORDING_MAX_PAUSE);
                let (current_term, commit_index) = {
                    let rf = rf.lock();
                    (rf.current_term, rf.commit_index)
                };
                this_daemon
                    .run_verify_authority_iteration(current_term, commit_index);
            }
            log::info!("{:?} verify authority daemon done.", me);
        }
    }

    /// Create a verify authority request. Returns None if we are not the
    /// leader.
    ///
    /// A successful verification allows the application to respond to read-only
    /// requests that arrived before this function is called. The answer must
    /// include all commands at or before a certain index, which is returned to
    /// the application with the successful verification result. The index is
    /// in fact the commit index at the moment this function was called. It is
    /// guaranteed that no other commands could possibly have been committed at
    /// the moment this function was called.
    ///
    /// The application is also free to include any subsequent commits in the
    /// response. Consistency is still guaranteed, because Raft never rolls back
    /// committed commands.
    pub fn verify_authority_async(
        &self,
    ) -> Option<impl Future<Output = crate::VerifyAuthorityResult>> {
        // Fail the request if we have been killed.
        if !self.keep_running.load(Ordering::Relaxed) {
            return None;
        }

        let (term, commit_index, last_index) = {
            let rf = self.inner_state.lock();
            if !rf.is_leader() {
                // Returning none instead of `Pending::Ready(TermElapsed)`,
                // because that requires a separate struct that implements
                // Future, which is tedious to write.
                return None;
            }

            (
                rf.current_term,
                rf.commit_index,
                rf.log.last_index_term().index,
            )
        };
        let receiver = self
            .verify_authority_daemon
            .verify_authority_async(term, commit_index);
        let force_heartbeat = commit_index == last_index;
        self.heartbeats_daemon.trigger(force_heartbeat);
        receiver.map(|receiver| async move {
            receiver
                .await
                .expect("Verify authority daemon never drops senders")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PEER_SIZE: usize = 5;
    const PAST_TERM: Term = Term(2);
    const TERM: Term = Term(3);
    const NEXT_TERM: Term = Term(4);
    const COMMIT_INDEX: Index = 8;

    fn init_daemon() -> VerifyAuthorityDaemon {
        let daemon = VerifyAuthorityDaemon::create(PEER_SIZE);

        daemon.reset_state(TERM, COMMIT_INDEX);

        const CURRENT_BEATS: [u64; 5] = [11, 9, 7, 5, 3];
        const TICKED: [u64; 5] = [0, 3, 1, 4, 2];
        for (index, beat_ticker) in daemon.beat_tickers.iter().enumerate() {
            for _ in 1..(PEER_SIZE - index) * 2 {
                beat_ticker.next_beat();
            }
            beat_ticker.tick(Beat((index * 3 % PEER_SIZE) as u64));

            assert_eq!(Beat(CURRENT_BEATS[index]), beat_ticker.current_beat());
            assert_eq!(Beat(TICKED[index]), beat_ticker.ticked());
        }

        daemon
    }

    macro_rules! assert_queue_len {
        ($daemon:expr, $len:expr) => {
            assert_eq!($len, $daemon.state.lock().queue.len());
        };
    }

    macro_rules! assert_ticket_ready {
        ($t:expr, $e:expr) => {{
            let mut receiver = $t.expect("Ticket should be valid");
            let result = receiver
                .try_recv()
                .expect("The receiver should be ready with the result");
            assert_eq!(result, $e);
            Some(receiver);
        }};
    }

    macro_rules! assert_ticket_pending {
        ($t:expr) => {{
            let mut receiver = $t.expect("Ticket should be valid");
            let err = receiver
                .try_recv()
                .expect_err("The receiver should not be ready");
            assert_eq!(err, tokio::sync::oneshot::error::TryRecvError::Empty);
            Some(receiver)
        }};
    }

    #[test]
    fn test_verify_authority_async() {
        let daemon = init_daemon();
        let ticket = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        ticket.expect("Getting ticket should not fail immediately");

        {
            let state = daemon.state.lock();
            assert_eq!(1, state.queue.len());
            let token = state.queue.get(0).unwrap();
            assert_eq!(
                [Beat(11), Beat(9), Beat(7), Beat(5), Beat(3)],
                token.beats_moment.as_slice()
            );
            assert_eq!(COMMIT_INDEX, token.commit_index);
        }

        daemon.beat_ticker(4).next_beat();
        daemon.beat_ticker(2).next_beat();
        daemon.verify_authority_async(TERM, COMMIT_INDEX + 10);

        {
            let state = daemon.state.lock();
            assert_eq!(2, state.queue.len());
            let token = state.queue.get(1).unwrap();
            assert_eq!(
                [Beat(11), Beat(9), Beat(8), Beat(5), Beat(4)],
                token.beats_moment.as_slice()
            );
            assert_eq!(COMMIT_INDEX + 10, token.commit_index);
        }
    }
    #[test]
    fn test_verify_authority_async_term_mismatch() {
        let daemon = init_daemon();
        let ticket =
            daemon.verify_authority_async(Term(TERM.0 + 1), COMMIT_INDEX);
        assert!(
            ticket.is_none(),
            "Should not issue a ticket for future terms"
        );

        let ticket =
            daemon.verify_authority_async(Term(TERM.0 - 1), COMMIT_INDEX);
        assert!(ticket.is_none(), "Should not issue a ticket for past terms");
        {
            let state = daemon.state.lock();
            assert_eq!(0, state.queue.len());
        }
    }

    #[test]
    fn test_reset_state() {
        let daemon = init_daemon();
        let t0 = daemon.verify_authority_async(TERM, COMMIT_INDEX - 2);
        let t1 = daemon.verify_authority_async(TERM, COMMIT_INDEX - 1);
        let t2 = daemon.verify_authority_async(TERM, COMMIT_INDEX);

        daemon.reset_state(NEXT_TERM, COMMIT_INDEX + 1);
        const CURRENT_BEATS: [u64; 5] = [12, 10, 8, 6, 4];
        for (index, beat_ticker) in daemon.beat_tickers.iter().enumerate() {
            assert_eq!(CURRENT_BEATS[index], beat_ticker.current_beat().0);
        }

        assert_ticket_ready!(t0, VerifyAuthorityResult::TermElapsed);
        assert_ticket_ready!(t1, VerifyAuthorityResult::TermElapsed);
        assert_ticket_ready!(t2, VerifyAuthorityResult::TermElapsed);

        let state = daemon.state.lock();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.start.0);
        assert_eq!(COMMIT_INDEX + 1, state.sentinel_commit_index);
        assert_eq!(NEXT_TERM, state.term);
        for covered in &state.covered {
            assert_eq!(0, covered.0);
        }
    }

    #[test]
    fn test_clear_ticked_requests() {
        let daemon = init_daemon();
        let beat_ticker0 = daemon.beat_tickers[0].clone();
        let beat_ticker1 = daemon.beat_tickers[1].clone();
        let beat_ticker2 = daemon.beat_tickers[2].clone();
        let beat_ticker3 = daemon.beat_tickers[3].clone();
        let beat_ticker4 = daemon.beat_tickers[4].clone();

        // An ancient tick that will be ticked at the end of the test.
        let beat2_ancient = beat_ticker2.ticked();

        let t0 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        // t0 receives beat2.
        let beat2 = beat_ticker2.next_beat();
        beat_ticker2.tick(beat2);
        // Run one iteration: one new tick is not enough.
        assert_queue_len!(&daemon, 1);
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        assert_queue_len!(&daemon, 1);

        let t1 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        let beat3_dup = beat_ticker3.current_beat();
        let beat3 = beat_ticker3.next_beat();
        assert_eq!(beat3.0, beat3_dup.0);
        // Run one iteration: one new tick for t0, zero for t1.
        assert_queue_len!(&daemon, 2);
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        let t0 = assert_ticket_pending!(t0);
        let t1 = assert_ticket_pending!(t1);
        assert_queue_len!(&daemon, 2);

        // Tick the same beat twice. t0 and t1 receives beat3.
        beat_ticker3.tick(beat3);
        beat_ticker3.tick(beat3_dup);
        // Run one iteration: two new ticks for t0, one for t1.
        assert_queue_len!(&daemon, 2);
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        // t0 is out.
        assert_queue_len!(&daemon, 1);
        assert_ticket_ready!(t0, VerifyAuthorityResult::Success(COMMIT_INDEX));
        let t1 = assert_ticket_pending!(t1);

        // t1 receives a beat from beat_ticker4.
        beat_ticker4.next_beat(); // a lost beat.
        beat_ticker4.tick(beat_ticker4.next_beat()); // an immediate beat.
        let beat4 = beat_ticker4.next_beat();
        let t2 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        let beat4_newest = beat_ticker4.next_beat();
        // Run one iteration: two new ticks for t1, zero for t2.
        assert_queue_len!(&daemon, 2);
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        // t1 is out.
        assert_queue_len!(&daemon, 1);
        assert_ticket_ready!(t1, VerifyAuthorityResult::Success(COMMIT_INDEX));
        let t2 = assert_ticket_pending!(t2);

        let t3 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        let beat0 = beat_ticker0.next_beat();
        // Not a new vote for t2: the beat is not recent enough.
        beat_ticker4.tick(beat4);
        let t4 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        assert_queue_len!(&daemon, 3);
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        assert_queue_len!(&daemon, 3);

        // t2, t3 and t4 all receive beat4_newest.
        // Two new votes for t2, one for t3 and one for t4.
        beat_ticker4.tick(beat4_newest);
        let beat1_stale = beat_ticker1.next_beat();
        let beat1 = beat_ticker1.next_beat();
        beat_ticker1.tick(beat1);
        assert_queue_len!(&daemon, 3);
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        // t2 is out
        assert_queue_len!(&daemon, 2);
        assert_ticket_ready!(t2, VerifyAuthorityResult::Success(COMMIT_INDEX));
        let t3 = assert_ticket_pending!(t3);
        let t4 = assert_ticket_pending!(t4);

        // New vote for t3, but not for t4.
        beat_ticker0.tick(beat0);
        // Stale beat for t3 and t4.
        beat_ticker1.tick(beat1_stale);
        // Ancient beat
        beat_ticker2.tick(beat2_ancient);
        assert_queue_len!(&daemon, 2);
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        // t3 is out
        assert_queue_len!(&daemon, 1);
        assert_ticket_ready!(t3, VerifyAuthorityResult::Success(COMMIT_INDEX));
        let t4 = assert_ticket_pending!(t4);

        // Many new votes for t4.
        beat_ticker1.tick(beat_ticker1.next_beat());
        beat_ticker2.tick(beat_ticker2.next_beat());
        beat_ticker3.tick(beat_ticker3.next_beat());
        beat_ticker4.tick(beat_ticker4.next_beat());
        assert_queue_len!(&daemon, 1);
        // Continue clearing the queue even if we are at a new term.
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        assert_queue_len!(&daemon, 0);
        assert_ticket_ready!(t4, VerifyAuthorityResult::Success(COMMIT_INDEX));
    }

    #[test]
    fn test_clear_ticked_requests_no_sentinel() {
        let daemon = init_daemon();
        daemon.state.lock().sentinel_commit_index = COMMIT_INDEX + 1;

        let t0 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        daemon.beat_tickers[3].tick(daemon.beat_tickers[3].next_beat());
        daemon.beat_tickers[4].tick(daemon.beat_tickers[4].next_beat());
        assert_queue_len!(&daemon, 1);

        // Note: sentinel is not committed.
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        assert_queue_len!(&daemon, 1);
        assert_ticket_pending!(t0);
    }

    #[test]
    fn test_clear_ticked_requests_lost_leadership() {
        let daemon = init_daemon();
        let t0 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        daemon.beat_tickers[3].tick(daemon.beat_tickers[3].next_beat());
        daemon.beat_tickers[4].tick(daemon.beat_tickers[4].next_beat());
        assert_queue_len!(&daemon, 1);
        // Note: this is at the next term.
        daemon.run_verify_authority_iteration(NEXT_TERM, COMMIT_INDEX);
        assert_queue_len!(&daemon, 0);
        assert_ticket_ready!(t0, VerifyAuthorityResult::Success(COMMIT_INDEX));
    }

    #[test]
    fn test_clear_ticked_requests_cleared_by_others() {
        let daemon = init_daemon();
        let _t0 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        let _t1 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        let t2 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        assert_queue_len!(&daemon, 3);
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        assert_queue_len!(&daemon, 3);

        {
            let mut state = daemon.state.lock();
            state.start = QueueIndex(2);
            state.queue.pop_front().expect("Queue should not be empty");
            state.queue.pop_front().expect("Queue should not be empty");
        }

        daemon.beat_tickers[0].tick(daemon.beat_tickers[0].next_beat());
        daemon.beat_tickers[1].tick(daemon.beat_tickers[1].next_beat());
        assert_queue_len!(&daemon, 1);
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        assert_queue_len!(&daemon, 0);

        assert_ticket_ready!(t2, VerifyAuthorityResult::Success(COMMIT_INDEX));
    }

    #[test]
    fn test_remove_expired_requests() {
        let daemon = init_daemon();
        let t0 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        let t1 = daemon.verify_authority_async(TERM, COMMIT_INDEX + 2);
        let t2 = daemon.verify_authority_async(TERM, COMMIT_INDEX + 1);
        let t3 = daemon.verify_authority_async(TERM, COMMIT_INDEX + 2);
        let t4 = daemon.verify_authority_async(TERM, COMMIT_INDEX + 1);

        // Override rough_time to test correctness.
        let now = Instant::now();
        {
            let mut state = daemon.state.lock();
            assert_eq!(5, state.queue.len());

            state.queue[0].rough_time = now - Duration::from_millis(1000);
            state.queue[1].rough_time = now - Duration::from_millis(500);
            state.queue[2].rough_time = now - Duration::from_millis(10);
            state.queue[3].rough_time = now - Duration::from_millis(1000);
            state.queue[4].rough_time = now;
        }

        // Run one iteration: no new commit, no new tick, for last term.
        daemon.run_verify_authority_iteration(PAST_TERM, COMMIT_INDEX);
        // Tokens should stay as-is.
        assert_queue_len!(&daemon, 5);

        // Run one iteration: no new commit, no new tick, for this term.
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        // Tokens should stay as-is.
        assert_queue_len!(&daemon, 5);

        // Run one iteration: no new commit, no new tick, for next term.
        daemon.run_verify_authority_iteration(NEXT_TERM, COMMIT_INDEX);

        assert_queue_len!(&daemon, 3);
        let queue = &daemon.state.lock().queue;
        assert_eq!(queue[0].rough_time, now - Duration::from_millis(10));
        // The token actually expired, but we should not remove it since it is
        // not at the beginning of the queue.
        assert_eq!(queue[1].rough_time, now - Duration::from_millis(1000));
        assert_eq!(queue[2].rough_time, now);

        assert_ticket_ready!(t0, VerifyAuthorityResult::TimedOut);
        assert_ticket_ready!(t1, VerifyAuthorityResult::TimedOut);
        assert_ticket_pending!(t2);
        assert_ticket_pending!(t3);
        assert_ticket_pending!(t4);
    }

    #[test]
    fn test_run_verify_authority_iteration() {
        let daemon = init_daemon();

        // Run of last term.
        daemon.reset_state(PAST_TERM, COMMIT_INDEX - 1);
        let _t0 = daemon.verify_authority_async(PAST_TERM, COMMIT_INDEX - 2);
        let _t1 = daemon.verify_authority_async(PAST_TERM, COMMIT_INDEX - 1);
        let _t2 = daemon.verify_authority_async(PAST_TERM, COMMIT_INDEX);
        daemon.run_verify_authority_iteration(PAST_TERM, COMMIT_INDEX - 1);

        // Run of current term.
        daemon.reset_state(TERM, COMMIT_INDEX);
        let beat_ticker0 = daemon.beat_tickers[0].clone();
        let beat_ticker1 = daemon.beat_tickers[1].clone();
        let beat_ticker2 = daemon.beat_tickers[2].clone();
        let beat_ticker3 = daemon.beat_tickers[3].clone();
        let beat_ticker4 = daemon.beat_tickers[4].clone();

        // New request t0.
        let t0 = daemon.verify_authority_async(TERM, COMMIT_INDEX - 1);
        // t0 has two votes.
        beat_ticker0.tick(beat_ticker0.next_beat());
        beat_ticker1.tick(beat_ticker1.next_beat());
        assert_queue_len!(&daemon, 1);

        // Do nothing since sentinel is not committed yet.
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX - 1);
        assert_queue_len!(&daemon, 1);

        // New request t1.
        let t1 = daemon.verify_authority_async(TERM, COMMIT_INDEX);
        // t1 has one vote.
        beat_ticker1.tick(beat_ticker1.next_beat());
        assert_queue_len!(&daemon, 2);

        // Clear t0 but not t1.
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        assert_queue_len!(&daemon, 1);
        // Cleared by the committed sentinel.
        assert_ticket_ready!(
            t0,
            VerifyAuthorityResult::Success(COMMIT_INDEX - 1)
        );

        // New requests t2 and t3.
        let t2 = daemon.verify_authority_async(TERM, COMMIT_INDEX + 1);
        // t1 has two notes, t2 has one.
        beat_ticker2.tick(beat_ticker2.next_beat());
        let t3 = daemon.verify_authority_async(TERM, COMMIT_INDEX + 1);
        // t1 has three votes, t2 has two, t3 has one.
        beat_ticker3.tick(beat_ticker3.next_beat());
        assert_queue_len!(&daemon, 3);

        // Clear t1 and t2 because they are ticked.
        daemon.run_verify_authority_iteration(TERM, COMMIT_INDEX);
        assert_queue_len!(&daemon, 1);
        // Note t0 and t1 have different commit indexes.
        assert_ticket_ready!(t1, VerifyAuthorityResult::Success(COMMIT_INDEX));
        assert_ticket_ready!(
            t2,
            VerifyAuthorityResult::Success(COMMIT_INDEX + 1)
        );

        // New request.
        let t4 = daemon.verify_authority_async(TERM, COMMIT_INDEX + 1);
        // t3 has two votes, t4 has one.
        beat_ticker4.tick(beat_ticker4.next_beat());

        // Make t3 and t4 expire.
        {
            let mut state = daemon.state.lock();
            let t3 =
                state.queue.front_mut().expect("Queue should not be empty");
            t3.rough_time = Instant::now() - Duration::from_secs(1);
            let t4 = state.queue.back_mut().expect("Queue should not be empty");
            t4.rough_time = Instant::now() - Duration::from_secs(1);
        }
        // Run for the next term.
        daemon.state.lock().sentinel_commit_index = COMMIT_INDEX + 2;
        daemon.run_verify_authority_iteration(NEXT_TERM, COMMIT_INDEX + 2);
        assert_queue_len!(&daemon, 0);
        assert_ticket_ready!(
            t3,
            VerifyAuthorityResult::Success(COMMIT_INDEX + 1)
        );
        assert_ticket_ready!(t4, VerifyAuthorityResult::TimedOut);
    }

    #[test]
    fn test_edge_case_stale_sentinel() {
        let daemon = init_daemon();
        // We were the leader at an earlier term.
        let stale_commit_index = COMMIT_INDEX;
        let _stale_sentinel_commit_index = COMMIT_INDEX;

        // Then we lost leadership. Someone became the leader and created new
        // entries. Those entries are committed, but we did not know.
        // So our commit index is not moved.
        let prev_term_log_index = COMMIT_INDEX + 2;
        // However, the new leader had answer queries at _prev_term_log_index.

        // We created a new sentinel, it is not yet committed.
        let sentinel_commit_index = COMMIT_INDEX + 3;

        // New term, we are the leader.
        daemon.reset_state(NEXT_TERM, sentinel_commit_index);
        let t = daemon.verify_authority_async(NEXT_TERM, COMMIT_INDEX);

        // We received 3 heartbeats.
        let beat_ticker0 = daemon.beat_tickers[0].clone();
        let beat_ticker1 = daemon.beat_tickers[1].clone();
        let beat_ticker2 = daemon.beat_tickers[2].clone();
        beat_ticker0.tick(beat_ticker0.next_beat());
        beat_ticker1.tick(beat_ticker1.next_beat());
        beat_ticker2.tick(beat_ticker2.next_beat());

        // We are now using stale data from the old term.
        daemon.run_verify_authority_iteration(TERM, stale_commit_index);
        let t = assert_ticket_pending!(t);

        // We are now using data from the new term.
        daemon.run_verify_authority_iteration(NEXT_TERM, sentinel_commit_index);
        assert_ticket_ready!(
            t,
            VerifyAuthorityResult::Success(prev_term_log_index)
        );
    }

    #[test]
    fn test_edge_case_stale_commit_index() {
        let daemon = init_daemon();
        // The previous leader created two new entries after COMMIT_INDEX. These
        // entries are committed, but we did not know. So our commit index is
        // not moved. However, the new leader had answer queries at
        // COMMIT_INDEX + 2.
        let prev_term_log_index = COMMIT_INDEX + 2;

        // We created a new sentinel, it is not yet committed.
        let sentinel_commit_index = COMMIT_INDEX + 3;

        // New term, we are the leader.
        daemon.reset_state(TERM, sentinel_commit_index);
        // Request `t` arrived.
        let stale_commit_index_for_t = COMMIT_INDEX;
        // The daemon is triggered.
        let stale_commit_index_for_daemon = sentinel_commit_index;
        // Request `_` arrived.
        let commit_index = sentinel_commit_index + 1;
        // This is a tricky order-of-order enqueue.
        let _ = daemon.verify_authority_async(TERM, commit_index);
        let t = daemon.verify_authority_async(TERM, stale_commit_index_for_t);

        // We received 3 heartbeats.
        let beat_ticker0 = daemon.beat_tickers[0].clone();
        let beat_ticker1 = daemon.beat_tickers[1].clone();
        let beat_ticker2 = daemon.beat_tickers[2].clone();
        beat_ticker0.tick(beat_ticker0.next_beat());
        beat_ticker1.tick(beat_ticker1.next_beat());
        beat_ticker2.tick(beat_ticker2.next_beat());

        // We are now using stale data from the new term.
        daemon.run_verify_authority_iteration(
            TERM,
            stale_commit_index_for_daemon,
        );
        assert_ticket_ready!(
            t,
            VerifyAuthorityResult::Success(prev_term_log_index)
        );
    }
}

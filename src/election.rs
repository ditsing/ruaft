use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use rand::{thread_rng, Rng};

use crate::remote_context::RemoteContext;
use crate::sync_log_entries::SyncLogEntriesComms;
use crate::utils::{retry_rpc, RPC_DEADLINE};
use crate::verify_authority::VerifyAuthorityDaemon;
use crate::{
    Peer, Persister, Raft, RaftState, ReplicableCommand, RequestVoteArgs,
    State, Term,
};

struct VersionedDeadline {
    version: usize,
    deadline: Option<Instant>,
}

pub(crate) struct ElectionState {
    // Timer will be removed upon shutdown or elected.
    timer: Mutex<VersionedDeadline>,
    // Wake up the timer thread when the timer is reset or cancelled.
    signal: Condvar,
}

const ELECTION_TIMEOUT_BASE_MILLIS: u64 = 200;
const ELECTION_TIMEOUT_VAR_MILLIS: u64 = 200;
impl ElectionState {
    pub(crate) fn create() -> Self {
        Self {
            timer: Mutex::new(VersionedDeadline {
                version: 0,
                deadline: None,
            }),
            signal: Condvar::new(),
        }
    }

    pub(crate) fn reset_election_timer(&self) {
        let mut guard = self.timer.lock();
        guard.version += 1;
        guard.deadline.replace(Self::election_timeout());
        self.signal.notify_one();
    }

    fn try_reset_election_timer(&self, timer_count: usize) -> bool {
        let mut guard = self.timer.lock();
        if guard.version != timer_count {
            return false;
        }
        guard.version += 1;
        guard.deadline.replace(Self::election_timeout());
        self.signal.notify_one();
        true
    }

    fn election_timeout() -> Instant {
        Instant::now()
            + Duration::from_millis(
                ELECTION_TIMEOUT_BASE_MILLIS
                    + thread_rng().gen_range(0..ELECTION_TIMEOUT_VAR_MILLIS),
            )
    }

    pub(crate) fn stop_election_timer(&self) {
        let mut guard = self.timer.lock();
        guard.version += 1;
        guard.deadline.take();
        self.signal.notify_one();
    }
}

/// A helper struct that stores all the information needed
/// to become the leader.
struct ElectionCandidate<Command> {
    me: Peer,
    term: Term,
    rf: Arc<Mutex<RaftState<Command>>>,
    election: Arc<ElectionState>,
    new_log_entry: SyncLogEntriesComms,
    verify_authority_daemon: VerifyAuthorityDaemon,
    persister: Arc<dyn Persister>,
}

// Command must be
// 0. 'static: Raft<Command> must be 'static, it is moved to another thread.
// 1. clone: they are copied to the persister.
// 2. send: Arc<Mutex<Vec<LogEntry<Command>>>> must be send, it is moved to another thread.
// 3. serialize: they are converted to bytes to persist.
impl<Command: ReplicableCommand> Raft<Command> {
    /// Runs the election timer daemon that triggers elections.
    ///
    /// The daemon holds a counter and an optional deadline in a mutex. Each
    /// time the timer is reset, the counter is increased by one. The deadline
    /// will be replaced with a randomized timeout. No other data is held.
    ///
    /// The daemon runs in a loop. In each iteration, the timer either fires, or
    /// is reset. At the beginning of each iteration, a new election will be
    /// started if
    /// 1. In the last iteration, the timer fired, and
    /// 2. Since the fired timer was set until now, the timer has not been
    /// reset, i.e. the counter has not been updated.
    ///
    /// If both conditions are met, an election is started. We keep a cancel
    /// token for the running election. Canceling is a courtesy and does not
    /// impact correctness. A should-have-been-cancelled election would cancel
    /// itself after counting enough votes ("term has changed").
    ///
    /// In each election, the first thing that happens is resetting the election
    /// timer. This reset and condition 2 above is tested and applied in the
    /// same atomic operation. Then one RPC is sent to each peer, asking for a
    /// vote. A task is created to wait for those RPCs to return and then count
    /// the votes.
    ///
    /// At the same time, the daemon locks the counter and the timeout. It
    /// expects the counter to increase by 1 but no more than that. If that
    /// expectation is not met, the daemon knows the election either did not
    /// happen, or the timer has been reset after the election starts. In that
    /// case it considers the timer not fired and skips the wait described
    /// below.
    ///
    /// If the expectation is met, the daemon waits util the timer fires, or
    /// the timer is reset, which ever happens first. If both happen when daemon
    /// wakes up, the reset takes precedence and the timer is considered not
    /// fired. The result (timer fired or is reset) is recorded so that it could
    /// be used in the next iteration.
    ///
    /// The daemon cancels the running election after waking up, no matter what
    /// happens. The iteration ends here.
    ///
    /// Before the first iteration, the timer is considered reset and not fired.
    ///
    /// The vote-counting task operates independently of the daemon. If it
    /// collects enough votes and the term has not yet passed, it resets the
    /// election timer. There could be more than one vote-counting tasks running
    /// at the same time, but all earlier tasks except the newest one will
    /// eventually realize the term they were competing for has passed and quit.
    pub(crate) fn run_election_timer(&self) {
        log::info!("{:?} election timer daemon running ...", self.me);

        let election = self.election.clone();

        let mut should_run = None;
        while self.keep_running.load(Ordering::Relaxed) {
            let mut cancel_handle = should_run.and_then(|last_timer_count| {
                self.run_election(last_timer_count)
            });

            let mut guard = election.timer.lock();
            let (timer_count, deadline) = (guard.version, guard.deadline);
            // If the timer is reset
            // 0. Zero times. We know should_run is None. If should_run has
            // a value, the election would have been started and the timer
            // reset by the election. That means the timer did not fire in
            // the last iteration. We should just wait.
            // 1. One time. We know that the timer is either reset by the
            // election or by someone else before the election, in which
            // case the election was never started. We should just wait.
            // 2. More than one time. We know that the timer is first reset
            // by the election, and then reset by someone else, in that
            // order. We should cancel the election and just wait.
            if let Some(last_timer_count) = should_run {
                let expected_timer_count = last_timer_count + 1;
                assert!(timer_count >= expected_timer_count);
                // If the timer was changed more than once, we know the
                // last scheduled election should have been cancelled.
                if timer_count > expected_timer_count {
                    cancel_handle.take().map(|c| c.send(()));
                }
            }
            // Check the running signal before sleeping. We are holding the
            // timer lock, so no one can change it. The kill() method will
            // not be able to notify this thread before `wait` is called.
            if !self.keep_running.load(Ordering::Relaxed) {
                cancel_handle.take().map(|c| c.send(()));
                break;
            }
            should_run = match deadline {
                Some(timeout) => loop {
                    let ret = election.signal.wait_until(&mut guard, timeout);
                    let fired = ret.timed_out() && Instant::now() > timeout;
                    // If the timer has been updated, do not schedule,
                    // break so that we could cancel.
                    if timer_count != guard.version {
                        // Timer has been updated, cancel current
                        // election, and block on timeout again.
                        break None;
                    } else if fired {
                        // Timer has fired, remove the timer and allow
                        // running the next election at timer_count.
                        // If the next election is cancelled before we
                        // are back on wait, timer_count will be set to
                        // a different value.
                        guard.version += 1;
                        guard.deadline.take();
                        break Some(guard.version);
                    }
                    // Alarm has not fired yet. Continue to wait.
                },
                None => {
                    // Nothing to do. Block here until the timer is reset.
                    // We will not block here indefinitely at shutdown, since
                    // we checked that keep_running is still true while holding
                    // the election lock.
                    election.signal.wait(&mut guard);
                    // The timeout has changed, check again.
                    None
                }
            };
            drop(guard);
            // Whenever woken up, cancel the current running election.
            // There are 3 cases we could reach here
            // 1. We received an AppendEntries, or decided to vote for
            // a peer, and thus turned into a follower. In this case we'll
            // be notified by the election signal.
            // 2. We are a follower but didn't receive a heartbeat on time,
            // or we are a candidate but didn't not collect enough vote on
            // time. In this case we'll have a timeout.
            // 3. When become a leader, or are shutdown. In this case we'll
            // be notified by the election signal.
            cancel_handle.map(|c| c.send(()));
        }

        log::info!("{:?} election timer daemon done.", self.me);
    }

    fn run_election(
        &self,
        timer_count: usize,
    ) -> Option<futures_channel::oneshot::Sender<()>> {
        let me = self.me;
        let (term, args) = {
            let mut rf = self.inner_state.lock();

            // The previous election is faster and reached the critical section
            // before us. We should stop and not run this election.
            // Or someone else increased the term and the timer is reset.
            if !self.election.try_reset_election_timer(timer_count) {
                return None;
            }

            rf.current_term.0 += 1;

            rf.voted_for = Some(me);
            rf.state = State::Candidate;

            self.persister.save_state(rf.persisted_state().into());

            let term = rf.current_term;
            let (last_log_index, last_log_term) =
                rf.log.last_index_term().unpack();

            (
                term,
                RequestVoteArgs {
                    term,
                    candidate_id: me,
                    last_log_index,
                    last_log_term,
                },
            )
        };

        let votes = self.spawn_request_votes(args);
        let (tx, rx) = futures_channel::oneshot::channel();
        let candidate = ElectionCandidate {
            me: self.me,
            term,
            rf: self.inner_state.clone(),
            election: self.election.clone(),
            new_log_entry: self.sync_log_entries_comms.clone(),
            verify_authority_daemon: self.verify_authority_daemon.clone(),
            persister: self.persister.clone(),
        };

        self.thread_pool.spawn(async move {
            if !Self::quorum_before_cancelled(votes, rx).await {
                return;
            }

            Self::become_leader(candidate);
        });
        Some(tx)
    }

    const REQUEST_VOTE_RETRY: usize = 1;
    async fn request_vote(peer: Peer, args: RequestVoteArgs) -> Option<bool> {
        let term = args.term;
        // See the comment in send_heartbeat() for this override.
        let rpc_client = RemoteContext::<Command>::rpc_client(peer);
        let reply =
            retry_rpc(Self::REQUEST_VOTE_RETRY, RPC_DEADLINE, move |_round| {
                rpc_client.request_vote(args.clone())
            })
            .await;
        if let Ok(reply) = reply {
            RemoteContext::<Command>::term_marker().mark(reply.term);
            return Some(reply.vote_granted && reply.term == term);
        }
        None
    }

    fn spawn_request_votes(
        &self,
        args: RequestVoteArgs,
    ) -> Vec<tokio::task::JoinHandle<Option<bool>>> {
        let mut votes = vec![];
        for peer in self.peers.clone().into_iter() {
            if peer != self.me {
                let one_vote = self
                    .thread_pool
                    .spawn(Self::request_vote(peer, args.clone()));
                votes.push(one_vote);
            }
        }
        return votes;
    }

    async fn quorum_before_cancelled(
        votes: Vec<tokio::task::JoinHandle<Option<bool>>>,
        cancel_token: futures_channel::oneshot::Receiver<()>,
    ) -> bool {
        let quorum = votes.len() >> 1;
        let mut vote_count = 0;
        let mut against_count = 0;
        let mut cancel_token = cancel_token;
        let mut futures_vec = votes;
        while vote_count < quorum
            && against_count <= quorum
            && !futures_vec.is_empty()
        {
            // Mixing tokio futures with futures-rs ones. Fingers crossed.
            let selected = futures_util::future::select(
                cancel_token,
                futures_util::future::select_all(futures_vec),
            )
            .await;
            let ((one_vote, _, rest), new_token) = match selected {
                futures_util::future::Either::Left(_) => break,
                futures_util::future::Either::Right(tuple) => tuple,
            };

            futures_vec = rest;
            cancel_token = new_token;

            if let Ok(Some(vote)) = one_vote {
                if vote {
                    vote_count += 1
                } else {
                    against_count += 1
                }
            }
        }

        return vote_count >= quorum;
    }

    fn become_leader(this: ElectionCandidate<Command>) {
        let term = this.term;
        let mut rf = this.rf.lock();
        if rf.current_term == term && rf.state == State::Candidate {
            // We are the leader now. The election timer can be stopped.
            this.election.stop_election_timer();

            rf.state = State::Leader;
            rf.leader_id = this.me;
            rf.match_index.fill(0);

            let sentinel_commit_index;
            if rf.commit_index != rf.log.last_index_term().index {
                // Create a sentinel commit at the start of the term.
                sentinel_commit_index = rf.log.add_term_change_entry(term);
                this.persister.save_state(rf.persisted_state().into());
            } else {
                sentinel_commit_index = rf.commit_index;
            }

            // Reset the verify authority daemon before sending heartbeats to
            // followers. This is critical to the correctness of verifying
            // authority.
            // No verity authority request can go through before the reset is
            // done, since we are holding the raft lock.
            this.verify_authority_daemon
                .reset_state(term, sentinel_commit_index);

            // Sync all logs now.
            this.new_log_entry
                .reset_progress(term, sentinel_commit_index);
        }
    }
}

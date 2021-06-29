use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use rand::{thread_rng, Rng};

use crate::utils::{retry_rpc, RPC_DEADLINE};
use crate::{Peer, Raft, RaftState, RequestVoteArgs, RpcClient, State, Term};

#[derive(Default)]
pub(crate) struct ElectionState {
    // Timer will be removed upon shutdown or elected.
    timer: Mutex<(usize, Option<Instant>)>,
    // Wake up the timer thread when the timer is reset or cancelled.
    signal: Condvar,
}

const ELECTION_TIMEOUT_BASE_MILLIS: u64 = 150;
const ELECTION_TIMEOUT_VAR_MILLIS: u64 = 250;
impl ElectionState {
    pub(crate) fn create() -> Self {
        Self {
            timer: Mutex::new((0, None)),
            signal: Condvar::new(),
        }
    }

    pub(crate) fn reset_election_timer(&self) {
        let mut guard = self.timer.lock();
        guard.0 += 1;
        guard.1.replace(Self::election_timeout());
        self.signal.notify_one();
    }

    fn try_reset_election_timer(&self, timer_count: usize) -> bool {
        let mut guard = self.timer.lock();
        if guard.0 != timer_count {
            return false;
        }
        guard.0 += 1;
        guard.1.replace(Self::election_timeout());
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
        guard.0 += 1;
        guard.1.take();
        self.signal.notify_one();
    }
}

// Command must be
// 0. 'static: Raft<Command> must be 'static, it is moved to another thread.
// 1. clone: they are copied to the persister.
// 2. send: Arc<Mutex<Vec<LogEntry<Command>>>> must be send, it is moved to another thread.
// 3. serialize: they are converted to bytes to persist.
impl<Command> Raft<Command>
where
    Command: 'static + Clone + Send + serde::Serialize,
{
    pub(crate) fn run_election_timer(&self) {
        let this = self.clone();
        let join_handle = std::thread::spawn(move || {
            // Note: do not change this to `let _ = ...`.
            let _guard = this.daemon_env.for_scope();

            let election = this.election.clone();

            let mut should_run = None;
            while this.keep_running.load(Ordering::SeqCst) {
                let mut cancel_handle = should_run
                    .map(|last_timer_count| this.run_election(last_timer_count))
                    .flatten();

                let mut guard = election.timer.lock();
                let (timer_count, deadline) = *guard;
                if let Some(last_timer_count) = should_run {
                    // If the timer was changed more than once, we know the
                    // last scheduled election should have been cancelled.
                    if timer_count > last_timer_count + 1 {
                        cancel_handle.take().map(|c| c.send(()));
                    }
                }
                // check the running signal before sleeping. We are holding the
                // timer lock, so no one can change it. The kill() method will
                // not be able to notify this thread before `wait` is called.
                if !this.keep_running.load(Ordering::SeqCst) {
                    break;
                }
                should_run = match deadline {
                    Some(timeout) => loop {
                        let ret =
                            election.signal.wait_until(&mut guard, timeout);
                        let fired = ret.timed_out() && Instant::now() > timeout;
                        // If the timer has been updated, do not schedule,
                        // break so that we could cancel.
                        if timer_count != guard.0 {
                            // Timer has been updated, cancel current
                            // election, and block on timeout again.
                            break None;
                        } else if fired {
                            // Timer has fired, remove the timer and allow
                            // running the next election at timer_count.
                            // If the next election is cancelled before we
                            // are back on wait, timer_count will be set to
                            // a different value.
                            guard.0 += 1;
                            guard.1.take();
                            break Some(guard.0);
                        }
                    },
                    None => {
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

            let stop_wait_group = this.stop_wait_group.clone();
            // Making sure the rest of `this` is dropped before the wait group.
            drop(this);
            drop(stop_wait_group);
        });
        self.daemon_env.watch_daemon(join_handle);
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

        let mut votes = vec![];
        for (index, rpc_client) in self.peers.iter().enumerate() {
            if index != self.me.0 {
                // RpcClient must be cloned so that it lives long enough for
                // spawn(), which requires static life time.
                let rpc_client = rpc_client.clone();
                // RPCs are started right away.
                let one_vote = self
                    .thread_pool
                    .spawn(Self::request_vote(rpc_client, args.clone()));
                votes.push(one_vote);
            }
        }

        let (tx, rx) = futures_channel::oneshot::channel();
        self.thread_pool.spawn(Self::count_vote_util_cancelled(
            me,
            term,
            self.inner_state.clone(),
            votes,
            rx,
            self.election.clone(),
            self.new_log_entry.clone().unwrap(),
        ));
        Some(tx)
    }

    const REQUEST_VOTE_RETRY: usize = 1;
    async fn request_vote(
        rpc_client: Arc<RpcClient>,
        args: RequestVoteArgs,
    ) -> Option<bool> {
        let term = args.term;
        // See the comment in send_heartbeat() for this override.
        let rpc_client = rpc_client.as_ref();
        let reply =
            retry_rpc(Self::REQUEST_VOTE_RETRY, RPC_DEADLINE, move |_round| {
                rpc_client.call_request_vote(args.clone())
            })
            .await;
        if let Ok(reply) = reply {
            return Some(reply.vote_granted && reply.term == term);
        }
        None
    }

    async fn count_vote_util_cancelled(
        me: Peer,
        term: Term,
        rf: Arc<Mutex<RaftState<Command>>>,
        votes: Vec<tokio::task::JoinHandle<Option<bool>>>,
        cancel_token: futures_channel::oneshot::Receiver<()>,
        election: Arc<ElectionState>,
        new_log_entry: std::sync::mpsc::Sender<Option<Peer>>,
    ) {
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

        if vote_count < quorum {
            return;
        }
        let mut rf = rf.lock();
        if rf.current_term == term && rf.state == State::Candidate {
            // We are the leader now. The election timer can be stopped.
            election.stop_election_timer();

            rf.state = State::Leader;
            rf.leader_id = me;
            let log_len = rf.log.end();
            for item in rf.next_index.iter_mut() {
                *item = log_len;
            }
            for item in rf.match_index.iter_mut() {
                *item = 0;
            }
            for item in rf.current_step.iter_mut() {
                *item = 0;
            }
            // Sync all logs now.
            let _ = new_log_entry.send(None);
        }
    }
}

use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::remote::RemoteContext;
use crate::utils::{retry_rpc, RPC_DEADLINE};
use crate::{AppendEntriesArgs, Peer, Raft, RaftState, ReplicableCommand};

pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);

#[derive(Clone)]
pub(crate) struct HeartbeatsDaemon {
    start: Instant,
    last_trigger: Arc<AtomicU64>,
    sender: tokio::sync::broadcast::Sender<()>,
}

impl HeartbeatsDaemon {
    const HEARTBEAT_MAX_DELAY_MILLIS: u64 = 30;

    pub fn create() -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(1);
        Self {
            start: Instant::now(),
            last_trigger: Arc::new(AtomicU64::new(0)),
            sender,
        }
    }

    pub fn trigger(&self, force: bool) {
        let now = self.start.elapsed().as_millis();
        // u64 is big enough for more than 500 million years.
        let now_lower_bits = (now & (u64::MAX) as u128) as u64;
        let last_trigger = self.last_trigger.load(Ordering::Acquire);
        let next_trigger =
            last_trigger.wrapping_add(Self::HEARTBEAT_MAX_DELAY_MILLIS);

        // Do not trigger heartbeats too frequently, unless we are forced.
        if force || next_trigger < now_lower_bits {
            let previous_trigger = self
                .last_trigger
                .fetch_max(now_lower_bits, Ordering::AcqRel);
            if last_trigger == previous_trigger {
                let _ = self.sender.send(());
            }
        }
    }
}

// Command must be
// 0. 'static: Raft<Command> must be 'static, it is moved to another thread.
// 1. clone: they are copied to the persister.
// 2. send: Arc<Mutex<Vec<LogEntry<Command>>>> must be send, it is moved to another thread.
// 3. serialize: they are converted to bytes to persist.
impl<Command: ReplicableCommand> Raft<Command> {
    /// Schedules tasks that send heartbeats to peers.
    ///
    /// One task is scheduled for each peer. The task sleeps for a duration
    /// specified by `interval`, wakes up, builds the request message to send
    /// and delegates the actual RPC-sending to another task before going back
    /// to sleep.
    ///
    /// The sleeping task does nothing if we are not the leader.
    ///
    /// The request message is a stripped down version of `AppendEntries`. The
    /// response from the peer is ignored.
    pub(crate) fn schedule_heartbeats(&self, interval: Duration) {
        // rf is now owned by the outer async function.
        let rf = self.inner_state.clone();
        // A on-demand trigger to sending a heartbeat.
        let mut trigger = self.heartbeats_daemon.sender.subscribe();
        // Shutdown signal.
        let keep_running = self.keep_running.clone();
        let peers = self.peers.clone();

        self.thread_pool.spawn(async move {
            let mut interval = tokio::time::interval(interval);
            while keep_running.load(Ordering::Relaxed) {
                let tick = pin!(interval.tick());
                let trigger = pin!(trigger.recv());
                let _ = futures_util::future::select(tick, trigger).await;
                if let Some(args) = Self::build_heartbeat(&rf) {
                    for peer in &peers {
                        tokio::spawn(Self::send_heartbeat(*peer, args.clone()));
                    }
                }
            }
        });
    }

    fn build_heartbeat(
        rf: &Mutex<RaftState<Command>>,
    ) -> Option<AppendEntriesArgs<Command>> {
        let rf = rf.lock();

        if !rf.is_leader() {
            return None;
        }

        let last_log = rf.log.last_index_term();
        let args = AppendEntriesArgs {
            term: rf.current_term,
            leader_id: rf.leader_id,
            prev_log_index: last_log.index,
            prev_log_term: last_log.term,
            entries: vec![],
            leader_commit: rf.commit_index,
        };
        Some(args)
    }

    const HEARTBEAT_RETRY: usize = 1;
    async fn send_heartbeat(
        peer: Peer,
        args: AppendEntriesArgs<Command>,
    ) -> std::io::Result<()> {
        let term = args.term;
        let beat_ticker = RemoteContext::<Command>::beat_ticker(peer);

        let beat = beat_ticker.next_beat();
        // Passing a reference that is moved to the following closure.
        //
        // It won't work if the rpc_client of type Arc is moved into the closure
        // directly. To clone the Arc, the function must own a mutable reference
        // to it. At the same time, rpc_client.call_append_entries() returns a
        // future that must own a reference, too. That caused a compiling error
        // of FnMut allowing "references to captured variables to escape".
        //
        // By passing-in a reference instead of an Arc, the closure becomes a Fn
        // (no Mut), which can allow references to escape.
        //
        // Another option is to use non-move closures, in which case rpc_client
        // of type Arc can be passed-in directly. However that requires args to
        // be sync because they can be shared by more than one futures.
        let rpc_client = RemoteContext::<Command>::rpc_client(peer);
        let response =
            retry_rpc(Self::HEARTBEAT_RETRY, RPC_DEADLINE, move |_round| {
                rpc_client.append_entries(args.clone())
            })
            .await?;
        if term == response.term {
            beat_ticker.tick(beat);
        } else {
            RemoteContext::<Command>::term_marker().mark(response.term);
        }
        Ok(())
    }
}

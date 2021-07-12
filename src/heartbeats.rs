use std::sync::atomic::Ordering;
use std::time::Duration;

use parking_lot::Mutex;

use crate::term_marker::TermMarker;
use crate::utils::{retry_rpc, RPC_DEADLINE};
use crate::{AppendEntriesArgs, Raft, RaftState, RemoteRaft};

// Command must be
// 0. 'static: Raft<Command> must be 'static, it is moved to another thread.
// 1. clone: they are copied to the persister.
// 2. send: Arc<Mutex<Vec<LogEntry<Command>>>> must be send, it is moved to another thread.
// 3. serialize: they are converted to bytes to persist.
impl<Command> Raft<Command>
where
    Command: 'static + Clone + Send + serde::Serialize,
{
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
        for (peer_index, rpc_client) in self.peers.iter().enumerate() {
            if peer_index != self.me.0 {
                // rf is now owned by the outer async function.
                let rf = self.inner_state.clone();
                // A function that updates term with responses to heartbeats.
                let term_marker = self.term_marker();
                // RPC client must be cloned into the outer async function.
                let rpc_client = rpc_client.clone();
                // Shutdown signal.
                let keep_running = self.keep_running.clone();
                self.thread_pool.spawn(async move {
                    let mut interval = tokio::time::interval(interval);
                    while keep_running.load(Ordering::SeqCst) {
                        interval.tick().await;
                        if let Some(args) = Self::build_heartbeat(&rf) {
                            tokio::spawn(Self::send_heartbeat(
                                rpc_client.clone(),
                                args,
                                term_marker.clone(),
                            ));
                        }
                    }
                });
            }
        }
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
        rpc_client: impl RemoteRaft<Command>,
        args: AppendEntriesArgs<Command>,
        term_watermark: TermMarker<Command>,
    ) -> std::io::Result<()> {
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
        let rpc_client = &rpc_client;
        let response =
            retry_rpc(Self::HEARTBEAT_RETRY, RPC_DEADLINE, move |_round| {
                rpc_client.append_entries(args.clone())
            })
            .await?;
        term_watermark.mark(response.term);
        Ok(())
    }
}

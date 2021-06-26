use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::utils::retry_rpc;
use crate::{AppendEntriesArgs, Raft, RaftState, RpcClient, RPC_DEADLINE};

// Command must be
// 0. 'static: Raft<Command> must be 'static, it is moved to another thread.
// 1. clone: they are copied to the persister.
// 2. send: Arc<Mutex<Vec<LogEntry<Command>>>> must be send, it is moved to another thread.
// 3. serialize: they are converted to bytes to persist.
impl<Command> Raft<Command>
where
    Command: 'static + Clone + Send + serde::Serialize,
{
    pub(crate) fn schedule_heartbeats(&self, interval: Duration) {
        for (peer_index, rpc_client) in self.peers.iter().enumerate() {
            if peer_index != self.me.0 {
                // rf is now owned by the outer async function.
                let rf = self.inner_state.clone();
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
        rpc_client: Arc<RpcClient>,
        args: AppendEntriesArgs<Command>,
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
        let rpc_client = rpc_client.as_ref();
        retry_rpc(Self::HEARTBEAT_RETRY, RPC_DEADLINE, move |_round| {
            rpc_client.call_append_entries(args.clone())
        })
        .await?;
        Ok(())
    }
}

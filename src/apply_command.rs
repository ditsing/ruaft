use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::daemon_env::Daemon;
use crate::{Index, Raft, Snapshot, HEARTBEAT_INTERVAL_MILLIS};

pub enum ApplyCommandMessage<Command> {
    Snapshot(Snapshot),
    Command(Index, Command),
}

pub trait ApplyCommandFnMut<Command>:
    'static + Send + FnMut(ApplyCommandMessage<Command>)
{
}

impl<Command, T: 'static + Send + FnMut(ApplyCommandMessage<Command>)>
    ApplyCommandFnMut<Command> for T
{
}

impl<Command> Raft<Command>
where
    Command: 'static + Clone + Send,
{
    /// Runs a daemon thread that sends committed log entries to the
    /// application via a callback `apply_command`.
    ///
    /// If we still have the log entries to apply, they will be sent to the
    /// application in a loop. Otherwise if the log entries to apply is
    /// covered by the current log snapshot, the snapshot will be installed.
    ///
    /// This daemon guarantees to send log entries and snapshots in increasing
    /// order of the log index.
    ///
    /// No assumption is made about the callback `apply_command`, with a few
    /// exceptions.
    /// * This daemon does not assume the log entry has been 'accepted' or
    /// 'applied' by the application when the callback returns.
    ///
    /// * The callback can block, although blocking is not recommended. The
    /// callback should not block forever, otherwise Raft will fail to shutdown
    /// cleanly.
    ///
    /// * The `apply_command` callback cannot fail. It must keep retrying until
    /// the current log entry is 'accepted'. Otherwise the next log entry cannot
    /// be delivered to the application.
    ///
    /// After sending each log entry to the application, this daemon notifies
    /// the snapshot daemon that there may be a chance to create a new snapshot.
    pub(crate) fn run_apply_command_daemon(
        &self,
        mut apply_command: impl ApplyCommandFnMut<Command>,
    ) {
        let keep_running = self.keep_running.clone();
        let me = self.me;
        let rf = self.inner_state.clone();
        let condvar = self.apply_command_signal.clone();
        let snapshot_daemon = self.snapshot_daemon.clone();
        let daemon_env = self.daemon_env.clone();
        let stop_wait_group = self.stop_wait_group.clone();
        let join_handle = std::thread::spawn(move || {
            // Note: do not change this to `let _ = ...`.
            let _guard = daemon_env.for_scope();
            log::info!("{:?} apply command daemon running ...", me);

            while keep_running.load(Ordering::SeqCst) {
                let messages = {
                    let mut rf = rf.lock();
                    if rf.last_applied >= rf.commit_index {
                        // We have applied all committed log entries, wait until
                        // new log entries are committed.
                        condvar.wait_for(
                            &mut rf,
                            Duration::from_millis(HEARTBEAT_INTERVAL_MILLIS),
                        );
                    }
                    if rf.last_applied < rf.log.start() {
                        let (index_term, data) = rf.log.snapshot();
                        let messages =
                            vec![ApplyCommandMessage::Snapshot(Snapshot {
                                last_included_index: index_term.index,
                                data: data.to_vec(),
                            })];
                        rf.last_applied = rf.log.start();
                        messages
                    } else if rf.last_applied < rf.commit_index {
                        let index = rf.last_applied + 1;
                        let last_one = rf.commit_index + 1;
                        // This is safe because commit_index is always smaller
                        // than log.end(), see COMMIT_INDEX_INVARIANT.
                        assert!(last_one <= rf.log.end());
                        let messages: Vec<ApplyCommandMessage<Command>> = rf
                            .log
                            .between(index, last_one)
                            .iter()
                            .map(|entry| {
                                ApplyCommandMessage::Command(
                                    entry.index,
                                    entry.command.clone(),
                                )
                            })
                            .collect();
                        rf.last_applied = rf.commit_index;
                        messages
                    } else {
                        continue;
                    }
                };

                // Release the lock while calling external functions.
                for message in messages {
                    apply_command(message);
                    snapshot_daemon.trigger();
                }
            }
            log::info!("{:?} apply command daemon done.", me);

            drop(stop_wait_group);
        });
        self.daemon_env
            .watch_daemon(Daemon::ApplyCommand, join_handle);
    }
}

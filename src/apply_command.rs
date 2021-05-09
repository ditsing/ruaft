use std::sync::atomic::Ordering;
use std::time::Duration;

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
    pub(crate) fn run_apply_command_daemon(
        &self,
        mut apply_command: impl ApplyCommandFnMut<Command>,
    ) -> std::thread::JoinHandle<()> {
        let keep_running = self.keep_running.clone();
        let rf = self.inner_state.clone();
        let condvar = self.apply_command_signal.clone();
        let snapshot_daemon = self.snapshot_daemon.clone();
        let stop_wait_group = self.stop_wait_group.clone();
        std::thread::spawn(move || {
            while keep_running.load(Ordering::SeqCst) {
                let messages = {
                    let mut rf = rf.lock();
                    if rf.last_applied >= rf.commit_index {
                        condvar.wait_for(
                            &mut rf,
                            Duration::from_millis(HEARTBEAT_INTERVAL_MILLIS),
                        );
                    }
                    if rf.last_applied < rf.log.start() {
                        rf.last_applied = rf.log.start();
                        let (index_term, data) = rf.log.snapshot();
                        vec![ApplyCommandMessage::Snapshot(Snapshot {
                            last_included_index: index_term.index,
                            data: data.to_vec(),
                        })]
                    } else if rf.last_applied < rf.commit_index {
                        let index = rf.last_applied + 1;
                        let last_one = rf.commit_index + 1;
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

            drop(stop_wait_group);
        })
    }
}

use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::{Index, Raft, HEARTBEAT_INTERVAL_MILLIS};

pub trait ApplyCommandFnMut<Command>:
    'static + Send + FnMut(Index, Command)
{
}

impl<Command, T: 'static + Send + FnMut(Index, Command)>
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
                let (mut index, commands) = {
                    let mut rf = rf.lock();
                    if rf.last_applied >= rf.commit_index {
                        condvar.wait_for(
                            &mut rf,
                            Duration::from_millis(HEARTBEAT_INTERVAL_MILLIS),
                        );
                    }
                    if rf.last_applied < rf.commit_index {
                        let index = rf.last_applied + 1;
                        let last_one = rf.commit_index + 1;
                        let commands: Vec<Command> = rf
                            .log
                            .between(index, last_one)
                            .iter()
                            .map(|entry| entry.command.clone())
                            .collect();
                        rf.last_applied = rf.commit_index;
                        (index, commands)
                    } else {
                        continue;
                    }
                };

                // Release the lock while calling external functions.
                for command in commands {
                    apply_command(index, command);
                    snapshot_daemon.trigger();
                    index += 1;
                }
            }

            drop(stop_wait_group);
        })
    }
}

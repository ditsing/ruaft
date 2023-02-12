use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::{Raft, ReplicableCommand};

impl<Command: ReplicableCommand> Raft<Command> {
    pub fn schedule_check_quorum(&self, interval: Duration) {
        let me = self.me;
        let keep_running = self.keep_running.clone();
        let rf = self.inner_state.clone();
        let election = self.election.clone();
        let persister = self.persister.clone();

        let verify_authority_daemon = self.verify_authority_daemon.clone();
        let heartbeats_daemon = self.heartbeats_daemon.clone();

        self.thread_pool.spawn(async move {
            let mut interval = tokio::time::interval(interval);

            while keep_running.load(Ordering::Relaxed) {
                let (is_leader, term) = {
                    let rf = rf.lock();
                    (rf.is_leader(), rf.current_term)
                };

                if !is_leader {
                    // Skip the rest of the loop if we are not the leader.
                    interval.tick().await;
                    continue;
                }

                // Technically we shouldn't get beats if we are not the leader,
                // but it does not hurt since we acquired the soft term lock.
                let beats_moment = verify_authority_daemon.beats_moment();

                heartbeats_daemon.trigger(false);
                interval.tick().await;

                // If we had authority in the past, that means we have not lost
                // contact with followers. Keep going.
                if verify_authority_daemon.verify_beats_moment(beats_moment) {
                    continue;
                }

                let mut rf = rf.lock();
                // Only step down if we are still the leader at the same term.
                if rf.is_leader() && rf.current_term == term {
                    log::warn!("Leader {me:?} lost quorum, stepping down.");

                    rf.step_down();
                    election.reset_election_timer();
                    persister.save_state(rf.persisted_state().into());
                }
            }
        });
    }
}

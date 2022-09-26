use crate::daemon_env::ThreadEnv;
use crossbeam_utils::sync::WaitGroup;

#[derive(Debug)]
pub(crate) enum Daemon {
    Snapshot,
    ElectionTimer,
    SyncLogEntries,
    ApplyCommand,
    VerifyAuthority,
}

/// A guard for daemons.
///
/// [`DaemonWatch`] manages daemon threads and makes sure that panics are
/// recorded during shutdown. It collects daemon panics and send them to
/// [`crate::DaemonEnv`].
pub(crate) struct DaemonWatch {
    daemons: Vec<(Daemon, std::thread::JoinHandle<()>)>,
    thread_env: ThreadEnv,
    stop_wait_group: WaitGroup,
}

impl DaemonWatch {
    pub fn create(thread_env: ThreadEnv) -> Self {
        Self {
            daemons: vec![],
            thread_env,
            stop_wait_group: WaitGroup::new(),
        }
    }

    /// Register a daemon thread to make sure it is correctly shutdown when the
    /// Raft instance is killed.
    pub fn create_daemon<F, T>(&mut self, daemon: Daemon, func: F)
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        let thread_env = self.thread_env.clone();
        let stop_wait_group = self.stop_wait_group.clone();
        let thread = std::thread::Builder::new()
            .name(format!("ruaft-daemon-{:?}", daemon))
            .spawn(move || {
                thread_env.attach();
                func();
                ThreadEnv::detach();
                drop(stop_wait_group);
            })
            .expect("Creating daemon thread should never fail");
        self.daemons.push((daemon, thread));
    }

    pub fn wait_for_daemons(self) {
        self.stop_wait_group.wait();
        self.thread_env.attach();
        for (daemon, join_handle) in self.daemons.into_iter() {
            if let Some(err) = join_handle.join().err() {
                let err_str = err.downcast_ref::<&str>().map(|s| s.to_owned());
                let err_string =
                    err.downcast_ref::<String>().map(|s| s.as_str());
                let err =
                    err_str.or(err_string).unwrap_or("unknown panic error");
                ThreadEnv::upgrade().record_panic(daemon, err);
            }
        }
        ThreadEnv::detach();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon_env::DaemonEnv;

    #[test]
    fn test_watch_daemon_shutdown() {
        let daemon_env = DaemonEnv::create();
        let mut daemon_watch = DaemonWatch::create(daemon_env.for_thread());
        daemon_watch.create_daemon(Daemon::ApplyCommand, || {
            panic!("message with type &str");
        });

        daemon_watch.create_daemon(Daemon::Snapshot, || {
            panic!("message with type {:?}", "debug string");
        });

        daemon_watch.wait_for_daemons();

        let result = std::thread::spawn(move || {
            daemon_env.shutdown();
        })
        .join();
        let message = result.expect_err("shutdown should have panicked");
        let message = message
            .downcast_ref::<String>()
            .expect("Error message should be a string.");
        assert_eq!(
            message,
            "\n2 daemon panic(s):\nDaemon ApplyCommand panicked: message with type &str\nDaemon Snapshot panicked: message with type \"debug string\"\n0 error(s):\n"
        );
    }
}

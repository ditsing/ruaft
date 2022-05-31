use std::cell::RefCell;
use std::sync::{Arc, Weak};

use crossbeam_utils::sync::WaitGroup;
use parking_lot::Mutex;

#[cfg(all(not(test), feature = "integration-test"))]
use test_utils::thread_local_logger::{self, LocalLogger};

use crate::{IndexTerm, Peer, RaftState, State, Term};

/// A convenient macro to record errors.
#[macro_export]
macro_rules! check_or_record {
    ($condition:expr, $error_kind:expr, $message:expr, $rf:expr) => {
        if !$condition {
            crate::daemon_env::ThreadEnv::upgrade().record_error(
                $error_kind,
                $message,
                $rf,
                concat!(file!(), ":", line!()),
            )
        }
    };
}

/// Environment for daemons.
///
/// Each daemon thread should hold a copy of this struct, either directly or
/// through a copy of [`crate::Raft`]. It can be used for logging unexpected
/// errors to a central location, which cause a failure at shutdown. It also
/// checks daemon thread panics and collect information if they do.
#[derive(Clone, Debug)]
pub(crate) struct DaemonEnv {
    data: Arc<Mutex<DaemonEnvData>>,
    thread_env: ThreadEnv,
    stop_wait_group: Option<WaitGroup>,
}

#[derive(Debug)]
pub(crate) enum Daemon {
    Snapshot,
    ElectionTimer,
    SyncLogEntries,
    ApplyCommand,
    VerifyAuthority,
}

#[derive(Debug, Default)]
struct DaemonEnvData {
    errors: Vec<Error>,
    daemons: Vec<(Daemon, std::thread::JoinHandle<()>)>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct Error {
    error_kind: ErrorKind,
    message: String,
    raft_state: StrippedRaftState,
    file_line: &'static str,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum ErrorKind {
    /// The leader sent log entries that do not match a committed log entry.
    /// It could be caused by a misbehaving leader, or that the leader should
    /// never have been elected, or that this Raft instance incorrectly moved
    /// its commit index.
    RollbackCommitted(usize),
    /// Similar to [`Self::RollbackCommitted`], but the leader sent a snapshot
    /// that is inconsistent with a committed log entry.
    SnapshotBeforeCommitted(usize, Term),
    /// The application sent a snapshot that contains items beyond the log end.
    SnapshotAfterLogEnd(usize),
    /// The application sent a snapshot that contains items that are not
    /// committed yet. Only committed items are sent to the application.
    SnapshotNotCommitted(usize),
    /// The recipient of the `InstallSnapshot` RPC should have been able to
    /// verify the term at index `.0` but did not. The index `.0` is after
    /// their commit index `.1`, and thus not yet committed or archived into a
    /// local snapshot. The recipient should still have the log entry at `.0`.
    RefusedSnapshotAfterCommitted(usize, usize),
    /// Similar to [`Self::RollbackCommitted`], but this error is logged by the
    /// leader after receiving a reply from a follower.
    DivergedBeforeCommitted(usize, usize),
    /// A follower committed a log entry that is different from the leader. An
    /// opportunistic check that looks for log mismatches, missing committed log
    /// entries or other corruptions.
    DivergedAtCommitted(usize),
    /// A follower received an AppendEntries RPC with a `prev_log_index` that
    /// is inconsistent with the index of entries included in the same RPC.
    AppendEntriesIndexMismatch(usize, Vec<IndexTerm>),
    /// A follower committed a log entry that is beyond the log end of the
    /// leader. An opportunistic check that looks for log mismatches, missing
    /// committed log entries or other corruptions.
    CommittedBeyondEnd(usize),
    /// When examining a sync log entry response from a follower, the leader
    /// noticed that a log entry that it sent out is no longer in its own log.
    LeaderLogShrunk(usize),
}

impl DaemonEnv {
    /// Record an error, with a stripped version of the state of this instance.
    /// Use macro `check_or_record` to auto populate the environment.
    pub fn record_error<T, S: AsRef<str>>(
        &self,
        error_kind: ErrorKind,
        message: S,
        raft_state: &RaftState<T>,
        file_line: &'static str,
    ) {
        self.data.lock().errors.push(Error {
            error_kind,
            message: message.as_ref().into(),
            raft_state: Self::strip_data(raft_state),
            file_line,
        })
    }

    /// Register a daemon thread to make sure it is correctly shutdown when the
    /// Raft instance is killed.
    pub fn watch_daemon<F, T>(&self, daemon: Daemon, func: F)
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        let thread_env = self.for_thread();
        let stop_wait_group = self
            .stop_wait_group
            .clone()
            .expect("Expecting a valid stop wait group when creating daemons");
        let thread = std::thread::Builder::new()
            .name(format!("ruaft-daemon-{:?}", daemon))
            .spawn(move || {
                thread_env.attach();
                func();
                ThreadEnv::detach();
                drop(stop_wait_group);
            })
            .expect("Creating daemon thread should never fail");
        self.data.lock().daemons.push((daemon, thread));
    }

    pub fn wait_for_daemons(&mut self) {
        if let Some(stop_wait_group) = self.stop_wait_group.take() {
            stop_wait_group.wait();
        } else {
            panic!("Daemons can only be waited once")
        }
    }

    /// Makes sure that all daemons have been shutdown, no more errors can be
    /// added, checks if any error has been added, or if any daemon panicked.
    pub fn shutdown(self) {
        let data = Arc::try_unwrap(self.data)
            .unwrap_or_else(|_| {
                panic!("No one should be holding daemon env at shutdown.")
            })
            .into_inner();
        let daemon_panics: Vec<String> = data
            .daemons
            .into_iter()
            .filter_map(|(daemon, join_handle)| {
                let err = join_handle.join().err()?;
                let err_str = err.downcast_ref::<&str>().map(|s| s.to_owned());
                let err_string =
                    err.downcast_ref::<String>().map(|s| s.as_str());
                let err =
                    err_str.or(err_string).unwrap_or("unknown panic error");
                Some(format!("\nDaemon {:?} panicked: {}", daemon, err))
            })
            .collect();
        let recorded_errors: Vec<String> = data
            .errors
            .iter()
            .map(|error| format!("\n{:?}", error))
            .collect();
        if !daemon_panics.is_empty() || !recorded_errors.is_empty() {
            // Do not panic again if we are cleaning up panicking threads.
            if std::thread::panicking() {
                eprintln!(
                    "\n{} daemon panic(s):{}\n{} error(s):{}\n",
                    daemon_panics.len(),
                    daemon_panics.join(""),
                    recorded_errors.len(),
                    recorded_errors.join("")
                )
            } else {
                panic!(
                    "\n{} daemon panic(s):{}\n{} error(s):{}\n",
                    daemon_panics.len(),
                    daemon_panics.join(""),
                    recorded_errors.len(),
                    recorded_errors.join("")
                )
            }
        }
    }

    fn strip_data<T>(raft: &RaftState<T>) -> StrippedRaftState {
        StrippedRaftState {
            current_term: raft.current_term,
            voted_for: raft.voted_for,
            log: raft.log.all_index_term(),
            commit_index: raft.commit_index,
            last_applied: raft.last_applied,
            state: raft.state,
            leader_id: raft.leader_id,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct StrippedRaftState {
    current_term: Term,
    voted_for: Option<Peer>,
    log: Vec<IndexTerm>,
    commit_index: usize,
    last_applied: usize,
    state: State,
    leader_id: Peer,
}

impl DaemonEnv {
    /// Creates a daemon environment. Each Raft instance should share the same
    /// environment. It should be added to any thread that executes Raft code.
    /// Use [`DaemonEnv::for_thread`] or [`DaemonEnv::for_scope`] to register
    /// the environment.
    pub fn create() -> Self {
        let data = Default::default();
        // Pre-create a template thread_env, so that we can clone the weak
        // pointer instead of downgrading frequently.
        let thread_env = ThreadEnv {
            data: Arc::downgrade(&data),
            #[cfg(all(not(test), feature = "integration-test"))]
            local_logger: thread_local_logger::get(),
        };

        Self {
            data,
            thread_env,
            stop_wait_group: Some(WaitGroup::new()),
        }
    }

    /// Creates a [`ThreadEnv`] that could be attached to a thread. Any code
    /// running in the thread can use this `DaemonEnv` to log errors. The thread
    /// must be stopped before `DaemonEnv::shutdown()` is called, otherwise it
    /// will panic when logging an error.
    pub fn for_thread(&self) -> ThreadEnv {
        self.thread_env.clone()
    }

    /// Creates a [`ThreadEnvGuard`] that registers this `DaemonEnv` in the
    /// current scope, which also remove it from the scope when dropped.
    pub fn for_scope(&self) -> ThreadEnvGuard {
        self.for_thread().attach();
        ThreadEnvGuard {}
    }
}

/// A weak reference to a [`DaemonEnv`] that is attached to a thread.
/// Use [`ThreadEnv::attach`] to consume this instance and attach to a thread,
/// and [`ThreadEnv::detach`] to undo that.
#[derive(Clone, Debug, Default)]
pub(crate) struct ThreadEnv {
    data: Weak<Mutex<DaemonEnvData>>,
    #[cfg(all(not(test), feature = "integration-test"))]
    local_logger: LocalLogger,
}

impl ThreadEnv {
    thread_local! {static ENV: RefCell<ThreadEnv> = Default::default()}

    /// Upgrade to the referenced [`DaemonEnv`].
    // The dance between Arc<> and Weak<> is complex, but useful:
    // 1) We do not have to worry about slow RPC threads causing
    // DaemonEnv::shutdown() to fail. They only hold a Weak<> pointer after all;
    // 2) We have one system that works both in the environments that we control
    // (daemon threads and our own thread pools), and in those we don't (RPC
    // handling methods);
    // 3) Utils (log_array, persister) can log errors without access to Raft;
    // 4) Because of 2), we do not need to expose DaemonEnv externally outside
    // this crate, even though there is a public macro referencing it.
    //
    // On the other hand, the cost is fairly small, because:
    // 1) Clone of weak is cheap: one branch plus one relaxed atomic load;
    // downgrade is more expensive, but we only do it once;
    // 2) Upgrade of weak is expensive, but that only happens when there is
    // an error, which should be (knock wood) rare;
    // 3) Set and unset a thread_local value is cheap, too.
    pub fn upgrade() -> DaemonEnv {
        let env = Self::ENV.with(|env| env.borrow().clone());
        DaemonEnv {
            data: env.data.upgrade().unwrap(),
            thread_env: env,
            stop_wait_group: None,
        }
    }

    /// Attach this instance to the current thread.
    pub fn attach(self) {
        #[cfg(all(not(test), feature = "integration-test"))]
        thread_local_logger::set(self.local_logger.clone());

        Self::ENV.with(|env| env.replace(self));
    }

    /// Detach the instance stored in the current thread.
    pub fn detach() {
        Self::ENV.with(|env| env.take());
    }
}

/// A guard that automatically cleans up the [`ThreadEnv`] attached to the
/// current thread when dropped. It does not restore the previous value.
pub(crate) struct ThreadEnvGuard {}

impl Drop for ThreadEnvGuard {
    fn drop(&mut self) {
        ThreadEnv::detach()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_same_env(local_env: DaemonEnv, daemon_env: DaemonEnv) {
        assert!(Arc::ptr_eq(&local_env.data, &daemon_env.data));
    }

    #[test]
    fn test_for_thread() {
        let daemon_env = DaemonEnv::create();
        let thread_env = daemon_env.for_thread();
        let join_handle = std::thread::spawn(|| {
            thread_env.attach();
            let local_env = ThreadEnv::upgrade();
            ThreadEnv::detach();
            local_env
        });
        let local_env = join_handle
            .join()
            .expect("local env should be the same as daemon_env");
        assert_same_env(local_env, daemon_env);
    }

    #[test]
    fn test_for_scope() {
        let daemon_env = DaemonEnv::create();
        let local_env = {
            let _guard = daemon_env.for_scope();
            ThreadEnv::upgrade()
        };
        assert_same_env(local_env, daemon_env);
        // A weak pointer with weak_count == 0 is a null weak pointer.
        ThreadEnv::ENV
            .with(|env| assert_eq!(env.borrow().data.weak_count(), 0));
    }

    #[test]
    fn test_record_error() {
        let daemon_env = DaemonEnv::create();
        {
            let _guard = daemon_env.for_scope();
            let state = RaftState::<i32>::create(1, Peer(0));
            check_or_record!(
                0 > 1,
                ErrorKind::SnapshotAfterLogEnd(1),
                "Just kidding",
                &state
            )
        }
        let guard = daemon_env.data.lock();
        let errors = &guard.errors;
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0].error_kind,
            ErrorKind::SnapshotAfterLogEnd(1)
        ));
        assert_eq!(&errors[0].message, "Just kidding");
    }

    #[test]
    fn test_watch_daemon_shutdown() {
        let daemon_env = DaemonEnv::create();
        daemon_env.watch_daemon(Daemon::ApplyCommand, || {
            panic!("message with type &str");
        });
        daemon_env.watch_daemon(Daemon::Snapshot, || {
            panic!("message with type {:?}", "debug string");
        });

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

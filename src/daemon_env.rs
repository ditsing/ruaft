use std::cell::RefCell;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::index_term::IndexTerm;
use crate::{Peer, RaftState, State, Term};

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
}

#[derive(Debug, Default)]
struct DaemonEnvData {
    errors: Vec<Error>,
    daemons: Vec<std::thread::JoinHandle<()>>,
}

#[derive(Debug)]
pub(crate) struct Error {
    error_kind: ErrorKind,
    message: String,
    raft_state: StrippedRaftState,
    file_line: &'static str,
}

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
    pub fn watch_daemon(&self, thread: std::thread::JoinHandle<()>) {
        self.data.lock().daemons.push(thread);
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
            .filter_map(|join_handle| {
                let err = join_handle.join().err()?;
                let err_str = err.downcast_ref::<&str>().map(|s| s.to_owned());
                let err_string =
                    err.downcast_ref::<String>().map(|s| s.as_str());
                let err =
                    err_str.or(err_string).unwrap_or("unknown panic error");
                Some("\n".to_owned() + err)
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
            log: raft.log.all().iter().map(|s| s.into()).collect(),
            commit_index: raft.commit_index,
            last_applied: raft.last_applied,
            state: raft.state,
            leader_id: raft.leader_id,
        }
    }
}

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
    pub(crate) fn create() -> Self {
        let data = Default::default();
        // Pre-create a template thread_env, so that we can clone the weak
        // pointer instead of downgrading frequently.
        let thread_env = ThreadEnv {
            data: Arc::downgrade(&data),
        };
        Self { data, thread_env }
    }

    /// Creates a [`ThreadEnv`] that could be attached to a thread. Any code
    /// running in the thread can use this `DaemonEnv` to log errors. The thread
    /// must be stopped before `DaemonEnv::shutdown()` is called, otherwise it
    /// will panic when logging an error.
    pub(crate) fn for_thread(&self) -> ThreadEnv {
        self.thread_env.clone()
    }

    /// Creates a [`ThreadEnvGuard`] that registers this `DaemonEnv` in the
    /// current scope, which also remove it from the scope when dropped.
    pub(crate) fn for_scope(&self) -> ThreadEnvGuard {
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
}

impl ThreadEnv {
    thread_local! {static ENV: RefCell<ThreadEnv> = Default::default()}

    /// Upgrade to the referenced [`DaemonEvn`].
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
        }
    }

    /// Attach this instance to the current thread.
    pub fn attach(self) {
        Self::ENV.with(|env| env.replace(self));
    }

    /// Detach the instance stored in the current thread.
    pub fn detach() {
        Self::ENV.with(|env| env.replace(Default::default()));
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

// TODO(ditsing): add tests.

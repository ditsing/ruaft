use std::marker::PhantomData;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::index_term::IndexTerm;
use crate::{Peer, RaftState, State, Term};

#[macro_export]
macro_rules! check_or_record {
    ($daemon_env:expr, $condition:expr, $component:expr, $error_kind:expr, $message:expr, $rf:expr) => {
        if !$condition {
            $daemon_env.record_error(
                $component,
                $error_kind,
                $message,
                $rf,
                concat!(file!(), ":", line!()),
            )
        }
    };
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DaemonEnv<T> {
    data: Arc<Mutex<DaemonEnvData<T>>>,
}

#[derive(Debug, Default)]
struct DaemonEnvData<T> {
    errors: Vec<Error>,
    daemons: Vec<std::thread::JoinHandle<()>>,
    phantom: PhantomData<T>,
}

#[derive(Debug)]
pub(crate) struct Error {
    component: Component,
    error_kind: ErrorKind,
    message: String,
    raft_state: StrippedRaftState,
    file_line: &'static str,
}

#[derive(Debug)]
pub(crate) enum ErrorKind {
    RollbackCommitted(usize),
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum Component {
    // Daemon threads.
    Election,
    SyncLogEntry,
    ApplyCommand,
    Snapshot,
    // Daemon tasks
    VoteCountingTask,
    SyncLogEntryTask,
    // RPC handlers
    InstallSnapshot,
    AppendEntries,
    RequestVote,
}

impl<T> DaemonEnv<T> {
    pub fn record_error<S: AsRef<str>>(
        &self,
        component: Component,
        error_kind: ErrorKind,
        message: S,
        raft_state: &RaftState<T>,
        file_line: &'static str,
    ) {
        self.data.lock().errors.push(Error {
            component,
            error_kind,
            message: message.as_ref().into(),
            raft_state: Self::strip_data(raft_state),
            file_line,
        })
    }

    pub fn watch_daemon(&self, thread: std::thread::JoinHandle<()>) {
        self.data.lock().daemons.push(thread);
    }

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
                let err_str = err
                    .downcast_ref::<&str>()
                    .map_or("unknown panic error", |s| s.to_owned());
                Some("\n".to_owned() + err_str)
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

    fn strip_data(raft: &RaftState<T>) -> StrippedRaftState {
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

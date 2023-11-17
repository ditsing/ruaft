use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::log_array::{LogArray, LogEntry};
use crate::raft_state::RaftState;
use crate::storage::decode_and_encode::{
    decode_log_entry, decode_voted_for, encode_log_entry, encode_voted_for,
};
use crate::storage::{
    RaftLogEntryRef, RaftStoragePersisterTrait, RaftStoredState,
};
use crate::{Index, IndexTerm, Peer, Term};

/// The internal interface of log entry persister. It is similar to
/// `RaftStoragePersisterTrait`, but with concrete and private types. It
/// provides better ergonomics to developers of this project.
///
/// This trait cannot contain generic methods because it is made into a trait
/// object in type `SharedLogPersister`. Trait objects are used to remove one
/// more generic parameter on the overall `Raft` type.
pub(crate) trait LogPersisterTrait<Command>: Send + Sync {
    /// Save term and vote from the RaftState.
    fn save_term_vote(&self, rf: &RaftState<Command>);

    /// Save one log entry. Blocking until the data is persisted.
    fn append_one_entry(&self, entry: &LogEntry<Command>);

    /// Save may log entries. Blocking until the data is persisted.
    fn append_entries(&self, entries: &[LogEntry<Command>]);

    /// Save snapshot. Blocking until the data is persisted.
    fn update_snapshot(&self, index_term: IndexTerm, snapshot: &[u8]);
}

/// A thin wrapper around `RaftStoragePersisterTrait`.
impl<T, Command> LogPersisterTrait<Command> for T
where
    Command: Serialize,
    T: RaftStoragePersisterTrait<LogEntry<Command>>,
{
    fn save_term_vote(&self, rf: &RaftState<Command>) {
        <T as RaftStoragePersisterTrait<LogEntry<Command>>>::save_term_vote(
            self,
            rf.current_term,
            encode_voted_for(&rf.voted_for),
        )
    }

    fn append_one_entry(&self, entry: &LogEntry<Command>) {
        <T as RaftStoragePersisterTrait<LogEntry<Command>>>::append_one_entry(
            self, entry,
        )
    }

    fn append_entries(&self, entries: &[LogEntry<Command>]) {
        <T as RaftStoragePersisterTrait<LogEntry<Command>>>::append_entries(
            self, entries,
        )
    }

    fn update_snapshot(&self, index_term: IndexTerm, snapshot: &[u8]) {
        <T as RaftStoragePersisterTrait<LogEntry<Command>>>::update_snapshot(
            self,
            index_term.index,
            index_term.term,
            snapshot,
        )
    }
}

/// The crate-internal interface that is exposed to other parts of this crate.
pub(crate) type SharedLogPersister<Command> =
    Arc<dyn LogPersisterTrait<Command>>;

/// Adapter from the internal `LogEntry` type to the public interface.
impl<Command: Serialize> RaftLogEntryRef for LogEntry<Command> {
    fn index(&self) -> Index {
        self.index
    }

    fn term(&self) -> Term {
        self.term
    }

    fn command_bytes(&self) -> Vec<u8> {
        encode_log_entry(self)
    }
}

impl RaftStoredState {
    pub(crate) fn current_term(&self) -> Term {
        self.current_term
    }

    pub(crate) fn voted_for(&self) -> Option<Peer> {
        decode_voted_for(&self.voted_for)
            .expect("Persisted log should not contain error")
    }

    pub(crate) fn restore_log_array<Command: DeserializeOwned>(
        self,
        log_array: &mut LogArray<Command>,
    ) {
        log_array.reset(self.snapshot_index, self.snapshot_term, self.snapshot);
        for entry in self.log.iter() {
            log_array.push(decode_log_entry(&entry.command));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::log_array::LogArray;
    use crate::raft::Peer;
    use crate::storage::decode_and_encode::{
        encode_log_entry, encode_voted_for,
    };
    use crate::storage::{RaftStoredLogEntry, RaftStoredState};
    use crate::{IndexTerm, Term};

    #[test]
    fn test_restore_log_array() {
        let mut log_array = LogArray::create();
        log_array.add_command(Term(1), 1i32);
        log_array.add_command(Term(3), 7i32);
        let stored = RaftStoredState {
            current_term: Term(8),
            voted_for: encode_voted_for(&Some(Peer(1))),
            log: vec![
                RaftStoredLogEntry {
                    index: 1,
                    term: Term(1),
                    command: encode_log_entry(log_array.at(1)),
                },
                RaftStoredLogEntry {
                    index: 2,
                    term: Term(3),
                    command: encode_log_entry(log_array.at(2)),
                },
            ],
            snapshot_index: 0,
            snapshot_term: Term(1),
            snapshot: vec![0x09, 0x90],
        };

        let mut new_log_array: LogArray<i32> = LogArray::create();
        stored.restore_log_array(&mut new_log_array);

        assert_eq!(log_array.start(), new_log_array.start());
        assert_eq!(log_array.end(), new_log_array.end());
        assert_eq!(log_array.at(1).command(), new_log_array.at(1).command());
        assert_eq!(log_array.at(2).command(), new_log_array.at(2).command());
        assert_eq!(IndexTerm::pack(0, Term(1)), new_log_array.snapshot().0);
        assert_eq!(&[0x09u8, 0x90u8], new_log_array.snapshot().1);
    }
}

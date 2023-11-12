use crate::{Index, Term};

/// A reference type that points to a Raft log entry. Used as input parameters
/// in the storage interface `RaftStoragePersisterTrait`.
/// This is to keep the implementation details of Raft log array separated from
/// the public storage interface.
pub trait RaftLogEntryRef {
    fn index(&self) -> Index;
    fn term(&self) -> Term;
    fn command_bytes(&self) -> Vec<u8>;
}

/// An object that writes data to the underlying storage. A typical disk-based
/// implementation can be implemented as follows:
/// 1. A file large enough to store a few integers: term, ID of voted for peer,
/// and a pair of disk offsets of valid log entries.
/// 2. A list of continuous disk blocks used to store an array of
/// `RaftStoredLogEntry` bytes.
/// 3. Another list of continuous disk blocks that stores the application
/// snapshot.
///
/// TODO: Add default index range check implementation to `append_one_entry()`
/// and `append_entries()`.
pub trait RaftStoragePersisterTrait<LogEntry: RaftLogEntryRef>:
    Send + Sync + 'static
{
    /// Save the term and vote to storage.
    fn save_term_vote(&self, term: Term, voted_for: String);

    /// Append one entry to the saved log, overriding the existing entry at the
    /// same index if it is previously appended. Any existing entries after the
    /// give index are discarded.
    fn append_one_entry(&self, entry: &LogEntry);

    /// Append a list of entries to the saved log, overriding existing entries
    /// at the same indexes if they are previously appended. The indexes
    /// specified in the input list must be in order and consecutive. Existing
    /// entries at indexes after the input index range are discarded.
    fn append_entries<'a, LogEntryList>(&self, entries: LogEntryList)
    where
        LogEntry: 'a,
        LogEntryList: IntoIterator<Item = &'a LogEntry>;

    /// Store the application snapshot. The snapshot is computed from all log
    /// entries at and before `index`. After the snapshot is saved, any log
    /// entry on or before index can be discarded.
    fn update_snapshot(&self, index: Index, snapshot: &[u8]);
}

/// An object that watches the underlying storage system and help Raft decide
/// if a log compaction, i.e. taking a snapshot, is needed.
pub trait RaftStorageMonitorTrait: Send + 'static {
    /// Returns true when the storage system requires a log compaction.
    fn should_compact_log_now(&self) -> bool;
}

/// A concrete type that holds one log entry read from the storage.
#[derive(Clone)]
pub struct RaftStoredLogEntry {
    pub index: Index,
    pub term: Term,
    pub command: Vec<u8>,
}

/// A concrete type that holds all information that is needed to restore the
/// Raft log array and application state right after the instance starts.
#[derive(Clone)]
pub struct RaftStoredState {
    pub current_term: Term,
    pub voted_for: String,
    pub log: Vec<RaftStoredLogEntry>,
    pub snapshot_index: Index,
    pub snapshot: Vec<u8>,
}

/// An object that has everything Raft needs from a storage system.
pub trait RaftStorageTrait {
    type RaftStoragePersister<LogEntry: RaftLogEntryRef>: RaftStoragePersisterTrait<LogEntry>;
    type RaftStorageMonitor: RaftStorageMonitorTrait;

    /// Returns a persister that writes data to the underlying storage.
    ///
    /// `LogEntry` is not a trait generic parameter, but a method generic parameter,
    /// because the implementation of this trait must accept any `RaftLogEntryRef`,
    /// even though it is guaranteed that at runtime only one concrete subtype of
    /// `RaftLogEntryRef` will be passed to the implementation.
    fn persister<LogEntry: RaftLogEntryRef>(
        self,
    ) -> std::sync::Arc<Self::RaftStoragePersister<LogEntry>>;

    /// Reads out the entire saved state, including term, vote, Raft logs and
    /// the application snapshot.
    fn read_state(&self) -> std::io::Result<RaftStoredState>;

    /// Whether or not this storage system requires log compaction. Any
    /// non-experimental storage must require log compaction.
    fn log_compaction_required(&self) -> bool {
        return true;
    }

    /// Returns a monitor that helps Raft decide when a compaction should happen
    /// during the lifetime of the application.
    fn monitor(&self) -> Self::RaftStorageMonitor;
}

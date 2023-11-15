/// A in-memory simulation of storage operations.
use std::mem::size_of;
use std::sync::Arc;

use parking_lot::Mutex;

use ruaft::storage::{
    RaftLogEntryRef, RaftStorageMonitorTrait, RaftStoragePersisterTrait,
    RaftStorageTrait, RaftStoredLogEntry, RaftStoredState,
};
use ruaft::{Index, Term};

#[derive(Clone)]
pub struct State {
    current_term: Term,
    voted_for: String,
    log: Vec<RaftStoredLogEntry>,

    snapshot_index: Index,
    snapshot: Vec<u8>,

    log_size: usize,
}

impl State {
    /// Create an empty saved instance.
    fn create() -> Self {
        Self {
            current_term: Term(0),
            voted_for: "".to_owned(),
            log: vec![],
            snapshot_index: 0,
            snapshot: vec![],
            log_size: 0,
        }
    }

    /// Append entry and update internal disk usage accounting.
    fn append_entry(&mut self, entry: RaftStoredLogEntry) {
        self.log_size += size_of::<RaftStoredLogEntry>();
        self.log_size += entry.command.len();

        self.log.push(entry);
    }

    /// Returns the total disk usage of stored data. Each scala type must be
    /// accounted here individually.
    fn total_size(&self) -> usize {
        self.log_size + self.voted_for.len() + size_of::<Self>()
    }
}

/// The shared data that should be put on disk.
pub struct InMemoryState(Mutex<State>);

/// The storage interface.
#[derive(Clone)]
pub struct InMemoryStorage {
    locked_state: Arc<InMemoryState>,
    max_state_bytes: usize,
}

impl RaftStorageTrait for InMemoryStorage {
    type RaftStoragePersister<LogEntry: RaftLogEntryRef> = InMemoryState;
    type RaftStorageMonitor = InMemoryStorageMonitor;

    fn persister<LogEntry>(self) -> Arc<Self::RaftStoragePersister<LogEntry>>
    where
        LogEntry: RaftLogEntryRef,
    {
        self.locked_state
    }

    fn read_state(&self) -> std::io::Result<RaftStoredState> {
        let stored = self.locked_state.0.lock();
        let snapshot_index = stored.snapshot_index;

        let mut organized_log = vec![];
        for op in &stored.log {
            if op.index <= snapshot_index {
                // Discard all entries that are before snapshot index.
                continue;
            }

            while organized_log
                .last()
                .map(|entry: &RaftStoredLogEntry| entry.index >= op.index)
                .unwrap_or(false)
            {
                organized_log.pop();
            }
            organized_log.push(RaftStoredLogEntry {
                index: op.index,
                term: op.term,
                command: op.command.clone(),
            });
        }

        Ok(RaftStoredState {
            current_term: stored.current_term,
            voted_for: stored.voted_for.clone(),
            log: organized_log,
            snapshot_index: stored.snapshot_index,
            snapshot: stored.snapshot.clone(),
        })
    }

    fn monitor(&self) -> Self::RaftStorageMonitor {
        InMemoryStorageMonitor {
            stored: self.locked_state.clone(),
            max_state_bytes: self.max_state_bytes,
        }
    }
}

/// The storage monitor interface and controls compaction.
pub struct InMemoryStorageMonitor {
    stored: Arc<InMemoryState>,
    max_state_bytes: usize,
}

impl RaftStorageMonitorTrait for InMemoryStorageMonitor {
    fn should_compact_log_now(&self) -> bool {
        let stored = self.stored.0.lock();
        let total_size = stored.total_size();
        return total_size > self.max_state_bytes;
    }
}

/// The persister interface that implements the logic.
impl<LogEntry: RaftLogEntryRef> RaftStoragePersisterTrait<LogEntry>
    for InMemoryState
{
    fn save_term_vote(&self, term: Term, voted_for: String) {
        let mut stored = self.0.lock();
        stored.current_term = term;
        stored.voted_for = voted_for;
    }

    fn append_one_entry(&self, entry: &LogEntry) {
        let mut stored = self.0.lock();
        stored.append_entry(RaftStoredLogEntry {
            index: entry.index(),
            term: entry.term(),
            command: entry.command_bytes(),
        });
    }

    fn append_entries<'a, LogEntryList>(&self, entries: LogEntryList)
    where
        LogEntry: 'a,
        LogEntryList: IntoIterator<Item = &'a LogEntry>,
    {
        let mut stored = self.0.lock();
        for entry in entries {
            stored.append_entry(RaftStoredLogEntry {
                index: entry.index(),
                term: entry.term(),
                command: entry.command_bytes(),
            })
        }
    }

    fn update_snapshot(&self, index: Index, snapshot: &[u8]) {
        let mut stored = self.0.lock();
        stored.snapshot_index = index;
        stored.snapshot = snapshot.to_vec();
    }
}

impl InMemoryStorage {
    /// Create a new storage with bytes limit.
    pub fn create(max_state_bytes: usize) -> Self {
        Self {
            locked_state: Arc::new(InMemoryState(Mutex::new(State::create()))),
            max_state_bytes,
        }
    }

    /// Save the entire in-memory state.
    pub fn save(&self) -> State {
        self.locked_state.0.lock().clone()
    }

    /// Restore the entire in-memory state, not including `max_state_bytes`.
    pub fn restore(&self, state: State) {
        *self.locked_state.0.lock() = state;
    }

    /// Returns the total bytes cost, not including snapshot.
    pub fn state_size(&self) -> usize {
        self.locked_state.0.lock().total_size()
    }

    /// Returns the bytes cost of the snapshot.
    pub fn snapshot_size(&self) -> usize {
        self.locked_state.0.lock().snapshot.len()
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;
    use std::ops::Deref;

    use parking_lot::Mutex;

    use ruaft::storage::{
        RaftLogEntryRef, RaftStorageMonitorTrait, RaftStoragePersisterTrait,
        RaftStorageTrait,
    };
    use ruaft::{Index, Term};

    use crate::in_memory_storage::{InMemoryState, State};
    use crate::InMemoryStorage;

    struct Transaction {
        index: Index,
        amount: f64,
        description: String,
    }

    impl Transaction {
        fn populate(index: Index) -> Self {
            Self {
                index,
                amount: index as f64 * 7.0,
                description: char::from('a' as u8 + index as u8).to_string(),
            }
        }
    }

    impl RaftLogEntryRef for Transaction {
        fn index(&self) -> Index {
            self.index
        }

        fn term(&self) -> Term {
            Term(self.index / 2)
        }

        fn command_bytes(&self) -> Vec<u8> {
            let mut bytes = vec![];
            bytes.extend(self.index.to_be_bytes());
            bytes.extend(self.amount.to_be_bytes());
            bytes.extend(self.description.bytes());

            bytes
        }
    }

    fn type_hint(
        val: &InMemoryState,
    ) -> &impl RaftStoragePersisterTrait<Transaction> {
        val
    }

    #[test]
    fn test_append() {
        let state = InMemoryState(Mutex::new(State::create()));
        state.append_one_entry(&Transaction {
            index: 0,
            amount: 0.0,
            description: "a".to_owned(),
        });

        state.append_entries(&[
            Transaction {
                index: 1,
                amount: 1.0,
                description: "test".to_owned(),
            },
            Transaction {
                index: 2,
                amount: -1.0,
                description: "another".to_owned(),
            },
            Transaction {
                index: 3,
                amount: 1.0,
                description: "test".to_owned(),
            },
        ]);

        state.append_one_entry(&Transaction {
            index: 1,
            amount: 2.0,
            description: "".to_owned(),
        });

        let state = state.0.lock();
        assert_eq!(0, state.current_term.0);
        assert!(state.voted_for.is_empty());
        assert_eq!(0, state.snapshot_index);

        assert!(state.snapshot.is_empty());
        assert_eq!(296, state.log_size);

        // log
        assert_eq!(5, state.log.len());

        // log[0]
        assert_eq!(0, state.log[0].index);
        assert_eq!(Term(0), state.log[0].term);
        assert_eq!(
            vec![
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // index
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // amount
                0x61, // "a"
            ],
            state.log[0].command
        );

        // log[1]
        let entry = &state.log[1];
        assert_eq!(1, entry.index);
        assert_eq!(Term(0), entry.term);
        assert_eq!(
            vec![
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // index
                0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // amount
                0x74, 0x65, 0x73, 0x74, // "test"
            ],
            entry.command
        );

        // log[2]
        let entry = &state.log[2];
        assert_eq!(2, entry.index);
        assert_eq!(Term(1), entry.term);
        assert_eq!(
            vec![
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // index
                0xBF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // amount
                0x61, 0x6E, 0x6F, 0x74, 0x68, 0x65, 0x72 // "another"
            ],
            entry.command
        );

        // log[3]
        let entry = &state.log[3];
        assert_eq!(3, entry.index);
        assert_eq!(Term(1), entry.term);
        assert_eq!(
            vec![
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // index
                0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // amount
                0x74, 0x65, 0x73, 0x74, // "test"
            ],
            entry.command
        );

        // log[4]
        let entry = &state.log[4];
        assert_eq!(1, entry.index);
        assert_eq!(Term(0), entry.term);
        assert_eq!(
            vec![
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // index
                0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // amount
            ],
            entry.command
        );
    }

    #[test]
    fn test_save_term_vote() {
        let state = InMemoryState(Mutex::new(State::create()));
        {
            let state = state.0.lock();
            assert_eq!(Term(0), state.current_term);
            assert!(state.voted_for.is_empty());
        }
        type_hint(&state).save_term_vote(Term(9), "hi".to_owned());

        let state = state.0.lock();
        assert_eq!(Term(9), state.current_term);
        assert_eq!("hi", &state.voted_for);
    }

    #[test]
    fn test_update_snapshot() {
        let state = InMemoryState(Mutex::new(State::create()));
        {
            let state = state.0.lock();
            assert_eq!(0, state.snapshot_index);
            assert!(state.snapshot.is_empty());
        }

        type_hint(&state).update_snapshot(7, &[0x01, 0x02]);

        let state = state.0.lock();
        assert_eq!(7, state.snapshot_index);
        assert_eq!(&[0x01, 0x02], state.snapshot.as_slice());
    }

    #[test]
    fn test_read_state() {
        let storage = InMemoryStorage::create(0);
        let state = storage.clone().persister::<Transaction>();
        state.append_entries(&[
            Transaction::populate(0),
            Transaction::populate(1),
            Transaction::populate(2),
            Transaction::populate(3),
            Transaction {
                index: 2,
                amount: 1.0,
                description: "hi".to_owned(),
            },
            Transaction::populate(4),
            Transaction::populate(5),
            Transaction::populate(5),
            Transaction::populate(5),
            Transaction::populate(6),
            Transaction {
                index: 3,
                amount: 1.0,
                description: "hi".to_owned(),
            },
            Transaction::populate(7),
            Transaction::populate(7),
            Transaction::populate(7),
        ]);
        type_hint(&state).save_term_vote(Term(7), "voted_for".to_owned());
        type_hint(&state).update_snapshot(1, &[0x99]);

        let raft_stored_state = storage
            .read_state()
            .expect("Read in-memory state should never fail");
        assert_eq!(Term(7), raft_stored_state.current_term);
        assert_eq!("voted_for", &raft_stored_state.voted_for);
        assert_eq!(3, raft_stored_state.log.len());
        assert_eq!(&[0x99], raft_stored_state.snapshot.as_slice());
        assert_eq!(1, raft_stored_state.snapshot_index);

        let entry = &raft_stored_state.log[0];
        assert_eq!(2, entry.index);
        assert_eq!(Term(1), entry.term);
        assert_eq!(
            &[
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // index
                0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // amount
                0x68, 0x69, // "hi"
            ],
            entry.command.as_slice()
        );

        let entry = &raft_stored_state.log[1];
        assert_eq!(3, entry.index);
        assert_eq!(Term(1), entry.term);
        assert_eq!(
            &[
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // index
                0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // amount
                0x68, 0x69, // "hi"
            ],
            entry.command.as_slice()
        );

        let entry = &raft_stored_state.log[2];
        assert_eq!(7, entry.index);
        assert_eq!(Term(3), entry.term);
        assert_eq!(
            &[
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // index
                0x40, 0x48, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, // amount
                0x68, // "h"
            ],
            entry.command.as_slice()
        );

        assert_eq!(905, state.0.lock().total_size());
    }

    #[test]
    fn test_save_restore() {
        let storage = InMemoryStorage::create(0);
        let state = storage.clone().persister::<Transaction>();
        state.append_one_entry(&Transaction {
            index: 9,
            amount: 1.0,
            description: "hello".to_owned(),
        });

        let saved = storage.save();

        let another_storage = InMemoryStorage::create(100);
        another_storage.restore(saved);

        assert_eq!(100, another_storage.max_state_bytes);
        let another_state = another_storage.locked_state.0.lock();
        let entry = &another_state.log[0];
        assert_eq!(9, entry.index);
        assert_eq!(Term(4), entry.term);
        assert_eq!(
            vec![
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, // index
                0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // amount
                0x68, 0x65, 0x6C, 0x6C, 0x6F, // "hello"
            ],
            entry.command
        );
    }

    #[test]
    fn test_total_size() {
        let state = State::create();
        assert_eq!(8, size_of::<Term>());
        assert_eq!(8, size_of::<Index>());
        assert_eq!(8, size_of::<usize>());
        assert_eq!(24, size_of::<String>());
        assert_eq!(24, size_of::<Vec<u8>>());

        // 104 = 8 + 24 + 24 + 8 + 24 + 8
        assert_eq!(96, state.total_size());

        let state = InMemoryState(Mutex::new(State::create()));
        // command_size = 8 + 8 + 5 = 21
        // log_size = 8 + 8 + 24 (vec) + command_size = 61
        state.append_one_entry(&Transaction {
            index: 9,
            amount: 1.0,
            description: "hello".to_owned(),
        });
        assert_eq!(61, state.0.lock().log_size);
        assert_eq!(96 + 61, state.0.lock().total_size());

        // total_size() is verified in other tests with complex setup.
    }

    #[test]
    fn test_monitor() {
        let storage = InMemoryStorage::create(150);
        let state = storage.clone().persister::<Transaction>();
        let monitor = storage.monitor();
        assert_eq!(150, monitor.max_state_bytes);
        assert!(!monitor.should_compact_log_now());

        state.append_one_entry(&Transaction {
            index: 9,
            amount: 1.0,
            description: "hello".to_owned(),
        });
        assert_eq!(157, storage.state_size());
        assert!(monitor.should_compact_log_now());

        let bigger_storage = InMemoryStorage::create(160);
        bigger_storage.restore(storage.save());
        assert_eq!(157, bigger_storage.state_size());
        let bigger_monitor = bigger_storage.monitor();
        assert!(!bigger_monitor.should_compact_log_now());
    }

    #[test]
    fn test_snapshot_size() {
        let storage = InMemoryStorage::create(0);
        let state = storage.clone().persister::<Transaction>();
        {
            let state = state.0.lock();
            assert_eq!(0, state.snapshot_index);
            assert!(state.snapshot.is_empty());
        }

        type_hint(state.deref()).update_snapshot(7, &[0x01, 0x02]);
        assert_eq!(2, storage.snapshot_size());
    }
}

use serde_derive::{Deserialize, Serialize};

use crate::index_term::IndexTerm;
use crate::Term;

/// A log array that stores a tail of the whole Raft log.
///
/// The Raft log represented by the log array has length `end()`. Only log
/// entries after `start()` are physically stored in the log array (with some
/// caveats). The index and term of log entries in range `[start(), end())` are
/// accessible. A snapshot is stored at the beginning of the log array, which
/// covers all commands before and **including** `start()`. The command at
/// `start()` is **not** accessible, but all commands after that are.
///
/// New entries can be appended to the end of the Raft log via `add_command()`
/// or `push()`.
///
/// The log array can be truncated to at most one entry, which is at `start()`
/// and contains the snapshot. The start of the log array can be shifted towards
/// `end()`, if another snapshot at that position is provided.
///
/// The log array can also be reset to a single entry, contains an index, a term
/// and a snapshot, via `reset()`.
///
/// The log array is guaranteed to have at least one element, containing an
/// index, a term and a snapshot.
///
/// All APIs **will** panic if the given index(es) are out of bound.

pub type Index = usize;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum LogEntryEnum<Command> {
    TermChange,
    Noop,
    Command(Command),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry<Command> {
    pub index: Index,
    pub term: Term,
    command: LogEntryEnum<Command>,
}

/// NOT THREAD SAFE.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct LogArray<C> {
    inner: Vec<LogEntry<C>>,
    snapshot: Vec<u8>,
}

#[derive(Debug)]
pub(crate) enum ValidationError {
    IndexMismatch(Index, Vec<IndexTerm>),
    TermSpike(Index, Vec<IndexTerm>),
    FutureTerm(Term, Index, Vec<IndexTerm>),
}

impl<C: Default> LogArray<C> {
    /// Create the initial Raft log with no user-supplied commands.
    pub fn create() -> LogArray<C> {
        let ret = LogArray {
            inner: vec![Self::build_first_entry(0, Term(0))],
            snapshot: vec![],
        };
        ret.check_one_element();
        ret
    }
}

impl<C> LogEntry<C> {
    pub fn command(&self) -> Option<&C> {
        match &self.command {
            LogEntryEnum::TermChange => None,
            LogEntryEnum::Noop => None,
            LogEntryEnum::Command(command) => Some(command),
        }
    }
}

// Log accessors
impl<C> LogArray<C> {
    /// The start of the stored log entries. The command at this index is no
    /// longer accessible, since it is included in the snapshot.
    pub fn start(&self) -> Index {
        self.first_entry().index
    }

    /// The end index of the Raft log.
    pub fn end(&self) -> Index {
        self.start() + self.inner.len()
    }

    /// The first index and term stored in this log array.
    pub fn first_index_term(&self) -> IndexTerm {
        self.first_entry().into()
    }

    /// The last index and term of the Raft log.
    pub fn last_index_term(&self) -> IndexTerm {
        self.last_entry().into()
    }

    /// The log entry at the given index.
    pub fn at(&self, index: Index) -> &LogEntry<C> {
        let index = self.check_at_index(index);
        &self.inner[index]
    }

    /// The first log entry on or after the given index.
    pub fn first_after(&self, index: Index) -> &LogEntry<C> {
        if index >= self.start() {
            self.at(index)
        } else {
            self.first_entry()
        }
    }

    /// All log entries after the given index.
    pub fn after(&self, index: Index) -> &[LogEntry<C>] {
        let index = self.check_range_index(index);
        &self.inner[index..]
    }

    /// All log entries in range [start, end).
    pub fn between(&self, start: Index, end: Index) -> &[LogEntry<C>] {
        let start = self.check_range_index(start);
        let end = self.check_range_index(end);
        &self.inner[start..end]
    }

    /// All log entries stored in the array.
    #[cfg(test)]
    pub fn all(&self) -> &[LogEntry<C>] {
        &self.inner[..]
    }

    /// `IndexTerm` of all log entries, without command.
    pub fn all_index_term(&self) -> Vec<IndexTerm> {
        self.inner.iter().map(|e| e.into()).collect()
    }

    /// The snapshot before and including `start()`.
    pub fn snapshot(&self) -> (IndexTerm, &[u8]) {
        (self.first_index_term(), &self.snapshot)
    }

    pub fn validate(&self, current_term: Term) -> Result<(), ValidationError> {
        let all_index_term = self.all_index_term();
        let (mut index, mut term) = all_index_term[0].clone().into();
        for entry in all_index_term[1..].iter() {
            index += 1;
            if entry.index != index {
                return Err(ValidationError::IndexMismatch(
                    index,
                    all_index_term,
                ));
            }
            if entry.term < term {
                return Err(ValidationError::TermSpike(index, all_index_term));
            }
            if entry.term > current_term {
                return Err(ValidationError::FutureTerm(
                    current_term,
                    index,
                    all_index_term,
                ));
            }
            term = entry.term;
        }
        Ok(())
    }
}

// Mutations
impl<C> LogArray<C> {
    /// Add a new entry to the Raft log. The new index is returned.
    fn add_entry(&mut self, term: Term, entry: LogEntryEnum<C>) -> Index {
        let index = self.end();
        self.push(LogEntry {
            index,
            term,
            command: entry,
        });
        index
    }

    /// Add a new entry to the Raft log, with term and command. The new index is
    /// returned.
    pub fn add_command(&mut self, term: Term, command: C) -> Index {
        self.add_entry(term, LogEntryEnum::Command(command))
    }

    /// Add a new term change entry to the Raft log. The new index is returned.
    pub fn add_term_change_entry(&mut self, term: Term) -> Index {
        self.add_entry(term, LogEntryEnum::TermChange)
    }

    /// Push a LogEntry into the Raft log. The index of the log entry must match
    /// the next index in the log.
    pub fn push(&mut self, log_entry: LogEntry<C>) {
        let index = log_entry.index;
        assert_eq!(index, self.end(), "Expecting new index to be exact at len");
        self.inner.push(log_entry);
        assert_eq!(
            index + 1,
            self.end(),
            "Expecting len increase by one after push",
        );
        assert_eq!(
            self.at(index).index,
            index,
            "Expecting pushed element to have the same index",
        );
        self.check_one_element();
    }

    /// Remove all log entries on and after `index`.
    pub fn truncate(&mut self, index: Index) {
        let index = self.check_middle_index(index);
        self.inner.truncate(index);
        self.check_one_element()
    }
}

impl<C: Default> LogArray<C> {
    /// Shift the start of the array to `index`, and store a new snapshot that
    /// covers all commands before and at `index`.
    pub fn shift(&mut self, index: Index, snapshot: Vec<u8>) {
        // Discard everything before index and store the snapshot.
        let offset = self.check_middle_index(index);
        // WARNING: Potentially all entries after offset would be copied.
        self.inner.drain(0..offset);
        self.snapshot = snapshot;

        // Override the first entry, we know there is at least one entry. This
        // is not strictly needed. One benefit is that the command can be
        // released after this point.
        let first = self.first_index_term();
        self.inner[0] = Self::build_first_entry(first.index, first.term);

        assert_eq!(
            first.index, index,
            "Expecting the first entry to have the same index."
        );

        self.check_one_element()
    }

    /// Reset the array to contain only one snapshot at the given `index` with
    /// the given `term`.
    pub fn reset(
        &mut self,
        index: Index,
        term: Term,
        snapshot: Vec<u8>,
    ) -> Vec<LogEntry<C>> {
        let mut inner = vec![Self::build_first_entry(index, term)];
        std::mem::swap(&mut inner, &mut self.inner);
        self.snapshot = snapshot;

        self.check_one_element();

        inner
    }
}

impl<C> LogArray<C> {
    fn first_entry(&self) -> &LogEntry<C> {
        self.inner
            .first()
            .expect("There must be at least one element in log")
    }

    fn last_entry(&self) -> &LogEntry<C> {
        self.inner
            .last()
            .expect("There must be at least one entry in log")
    }

    fn offset(&self, index: Index) -> usize {
        index - self.start()
    }

    fn check_at_index(&self, index: Index) -> usize {
        assert!(
            index >= self.start() && index < self.end(),
            "Accessing start log index {} out of range [{}, {})",
            index,
            self.start(),
            self.end()
        );

        self.offset(index)
    }

    fn check_range_index(&self, index: Index) -> usize {
        assert!(
            index >= self.start() && index <= self.end(),
            "Accessing end log index {} out of range [{}, {}]",
            index,
            self.start(),
            self.end()
        );

        self.offset(index)
    }

    fn check_middle_index(&self, index: Index) -> usize {
        assert!(
            index > self.start() && index < self.end(),
            "Log index {} out of range ({}, {})",
            index,
            self.start(),
            self.end()
        );

        self.offset(index)
    }

    #[allow(clippy::len_zero)]
    fn check_one_element(&self) {
        assert!(
            self.inner.len() >= 1,
            "There must be at least one element in log"
        )
    }
}

impl<C> LogArray<C> {
    fn build_first_entry(index: Index, term: Term) -> LogEntry<C> {
        LogEntry {
            index,
            term,
            command: LogEntryEnum::Noop,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use super::*;

    impl<C> std::ops::Index<usize> for LogArray<C> {
        type Output = LogEntry<C>;

        fn index(&self, index: usize) -> &Self::Output {
            self.at(index)
        }
    }

    fn make_log_array(len: usize) -> LogArray<i32> {
        make_log_array_range(0, len)
    }

    fn make_log_array_range(start: usize, end: usize) -> LogArray<i32> {
        let mut ret = vec![];
        for i in start..end {
            ret.push(LogEntry {
                term: Term(i / 3),
                index: i,
                command: LogEntryEnum::Command((end - i) as i32),
            })
        }

        LogArray {
            inner: ret,
            snapshot: vec![1, 2, 3],
        }
    }

    fn default_log_array() -> (usize, usize, LogArray<i32>) {
        (8, 17, make_log_array_range(8, 17))
    }

    #[test]
    fn test_create() {
        let log = LogArray::<i32>::create();
        log.check_one_element();

        assert_eq!(1, log.end());
        assert_eq!((0, Term(0)), log.first_index_term().into());
        assert_eq!(LogEntryEnum::Noop, log[0].command);
    }

    #[test]
    fn test_start() {
        let log = make_log_array_range(9, 17);
        assert_eq!(9, log.start());

        let log = make_log_array(9);
        assert_eq!(0, log.start());
    }

    #[test]
    fn test_end() {
        let log = make_log_array(7);
        assert_eq!(7, log.end());

        let log = make_log_array_range(8, 17);
        assert_eq!(17, log.end());
    }

    #[test]
    fn test_accessors() {
        let (start, end, log) = default_log_array();
        assert_eq!((start, Term(2)), log.first_index_term().into());
        assert_eq!((end - 1, Term(5)), log.last_index_term().into());
        assert_eq!(
            ((start, Term(2)).into(), [1, 2, 3].as_ref()),
            log.snapshot()
        );

        let all = log.all();
        assert_eq!(end - start, all.len());
        for i in start..end {
            assert_eq!(all[i - start].index, i);
        }
    }

    #[test]
    fn test_at() {
        let (start, end, log) = default_log_array();

        let last = log.at(end - 1);
        assert_eq!(end - 1, last.index);
        assert_eq!(5, last.term.0);
        assert_eq!(LogEntryEnum::Command(1), last.command);

        let first = log.at(start);
        assert_eq!(start, first.index);
        assert_eq!(2, first.term.0);
        assert_eq!(LogEntryEnum::Command(9), first.command);

        assert!(start < 12);
        assert!(end > 12);
        let middle = log.at(12);
        assert_eq!(12, middle.index);
        assert_eq!(4, middle.term.0);
        assert_eq!(LogEntryEnum::Command(5), middle.command);

        let at_before_start = catch_unwind(|| {
            log.at(start - 1);
        });
        assert!(at_before_start.is_err());
        let at_after_end = catch_unwind(|| {
            log.at(end);
        });
        assert!(at_after_end.is_err());
    }

    #[test]
    fn test_first_after() {
        let (start, _, log) = default_log_array();
        assert_eq!(log.first_after(0).index, log.first_entry().index);
        assert_eq!(log.first_after(start).index, log.at(start).index);
        assert_eq!(log.first_after(start + 1).index, log.at(start + 1).index);
        assert_ne!(log.first_after(0).index, log.first_after(start + 1).index);
    }

    #[test]
    fn test_index_operator() {
        let (start, end, log) = default_log_array();

        for i in start..end {
            assert_eq!(log[i].index, log.at(i).index);
            assert_eq!(log[i].term, log.at(i).term);
            assert_eq!(log[i].command, log.at(i).command);
        }
        assert!(catch_unwind(|| log[0].term).is_err());
        assert!(catch_unwind(|| log[20].term).is_err());
    }

    #[test]
    fn test_after() {
        let (start, end, log) = default_log_array();

        let offset = 12;
        assert!(offset > start);
        assert!(offset < end);

        let after = log.after(offset);
        assert_eq!(end - offset, after.len());
        for i in offset..end {
            assert_eq!(after[i - offset].index, i);
            assert_eq!(after[i - offset].term.0, i / 3);
        }

        assert!(catch_unwind(|| log.after(start - 1)).is_err());
        assert!(catch_unwind(|| log.after(start)).is_ok());
        assert!(catch_unwind(|| log.after(end)).is_ok());
        assert!(catch_unwind(|| log.after(end + 1)).is_err());
    }

    #[test]
    fn test_between() {
        let (start, end, log) = default_log_array();

        let between = log.between(start + 2, end);
        assert_eq!(end - start - 2, between.len());
        for i in start + 2..end {
            assert_eq!(between[i - start - 2].index, i);
            assert_eq!(between[i - start - 2].term.0, i / 3);
        }

        assert!(catch_unwind(|| log.between(start - 1, end)).is_err());
        assert!(catch_unwind(|| log.between(start + 2, end + 1)).is_err());
        assert!(catch_unwind(|| log.between(start, end)).is_ok());
    }

    #[test]
    fn test_add_command() {
        let (_, end, mut log) = default_log_array();
        let index = log.add_command(Term(8), 9);
        assert_eq!(8, log.at(index).term.0);
        assert_eq!(LogEntryEnum::Command(9), log.at(index).command);
        assert_eq!(index, end);
        assert_eq!(index + 1, log.end());
    }

    #[test]
    fn test_add_term_change() {
        let (_, end, mut log) = default_log_array();
        let index = log.add_term_change_entry(Term(8));
        assert_eq!(8, log.at(index).term.0);
        assert_eq!(LogEntryEnum::TermChange, log.at(index).command);
        assert_eq!(index, end);
        assert_eq!(index + 1, log.end());
    }

    #[test]
    fn test_push() {
        let (_, end, mut log) = default_log_array();
        log.push(LogEntry {
            term: Term(8),
            index: end,
            command: LogEntryEnum::Command(1),
        });
        assert_eq!(8, log.at(end).term.0);
        assert_eq!(LogEntryEnum::Command(1), log.at(end).command);
        assert_eq!(end + 1, log.end());
    }

    #[test]
    #[should_panic]
    fn test_push_small_index() {
        let (_, end, mut log) = default_log_array();
        log.push(LogEntry {
            term: Term(8),
            index: end - 1,
            command: LogEntryEnum::Command(1),
        });
    }

    #[test]
    #[should_panic]
    fn test_push_big_index() {
        let (_, end, mut log) = default_log_array();
        log.push(LogEntry {
            term: Term(8),
            index: end + 1,
            command: LogEntryEnum::Command(1),
        });
    }

    #[test]
    fn test_truncate() {
        let (start, _, mut log) = default_log_array();
        log.truncate(start + 5);
        assert_eq!(start + 5, log.end());
        for i in start..start + 5 {
            assert_eq!(log[i].index, i);
            assert_eq!(log[i].term.0, i / 3);
        }

        log.truncate(start + 1);
        assert_eq!(1, log.all().len());
    }

    #[test]
    #[should_panic]
    fn test_truncate_at_start() {
        let (start, _, mut log) = default_log_array();
        log.truncate(start);
    }

    #[test]
    #[should_panic]
    fn test_truncate_at_end() {
        let (_, end, mut log) = default_log_array();
        log.truncate(end);
    }

    #[test]
    #[should_panic]
    fn test_truncate_before_start() {
        let (start, _, mut log) = default_log_array();
        log.truncate(start - 1);
    }

    #[test]
    #[should_panic]
    fn test_truncate_after_end() {
        let (_, end, mut log) = default_log_array();
        log.truncate(end + 1);
    }

    #[test]
    fn test_shift() {
        let (start, end, mut log) = default_log_array();
        let offset = 10;
        assert!(offset > start);
        assert!(offset < end);

        log.shift(offset, vec![]);

        assert_eq!((offset, Term(3)), log.first_index_term().into());
        assert_eq!(LogEntryEnum::Noop, log[offset].command);

        let all = log.all();
        assert_eq!(end - offset, all.len());
        for i in offset..end {
            assert_eq!(i, all[i - offset].index);
            assert_eq!(i / 3, all[i - offset].term.0);
        }

        assert_eq!(log.snapshot, vec![]);
    }

    #[test]
    #[should_panic]
    fn test_shift_before_start() {
        let (start, end, mut log) = default_log_array();
        assert!(start < end);
        log.shift(start - 1, vec![]);
    }

    #[test]
    #[should_panic]
    fn test_shift_at_start() {
        let (start, end, mut log) = default_log_array();
        assert!(start < end);
        log.shift(start, vec![]);
    }

    #[test]
    #[should_panic]
    fn test_shift_at_end() {
        let (start, end, mut log) = default_log_array();
        assert!(start < end);
        log.shift(end, vec![]);
    }

    #[test]
    #[should_panic]
    fn test_shift_after_end() {
        let (start, end, mut log) = default_log_array();
        assert!(start < end);
        log.shift(end + 1, vec![]);
    }

    #[test]
    fn test_reset() {
        let (start, end, mut log) = default_log_array();
        let dump = log.reset(88, Term(99), vec![1, 2]);
        assert_eq!(1, log.all().len());
        assert_eq!(vec![1, 2], log.snapshot);
        assert_eq!(88, log[88].index);
        assert_eq!(99, log[88].term.0);
        assert_eq!(LogEntryEnum::Noop, log[88].command);

        assert_eq!(end - start, dump.len());
    }

    #[test]
    fn test_private_accessors() {
        let (start, end, log) = default_log_array();
        let first = log.first_entry();
        assert_eq!(start, first.index);
        assert_eq!(start / 3, first.term.0);

        let last = log.last_entry();
        assert_eq!(end - 1, last.index);
        assert_eq!((end - 1) / 3, last.term.0);

        assert_eq!(10 - start, log.offset(10));
    }

    #[test]
    fn test_check_start_index() {
        let (start, end, log) = default_log_array();
        assert!(start < end);
        assert!(catch_unwind(|| log.check_at_index(start - 8)).is_err());
        assert!(catch_unwind(|| log.check_at_index(start - 1)).is_err());
        assert!(catch_unwind(|| log.check_at_index(start)).is_ok());
        assert!(catch_unwind(|| log.check_at_index(start + 1)).is_ok());
        assert!(catch_unwind(|| log.check_at_index(end - 1)).is_ok());
        assert!(catch_unwind(|| log.check_at_index(end)).is_err());
        assert!(catch_unwind(|| log.check_at_index(end + 1)).is_err());
        assert!(catch_unwind(|| log.check_at_index(end + 5)).is_err());
    }

    #[test]
    fn test_check_range_index() {
        let (start, end, log) = default_log_array();
        assert!(start < end);
        assert!(catch_unwind(|| log.check_range_index(start - 8)).is_err());
        assert!(catch_unwind(|| log.check_range_index(start - 1)).is_err());
        assert!(catch_unwind(|| log.check_range_index(start)).is_ok());
        assert!(catch_unwind(|| log.check_range_index(start + 1)).is_ok());
        assert!(catch_unwind(|| log.check_range_index(end - 1)).is_ok());
        assert!(catch_unwind(|| log.check_range_index(end)).is_ok());
        assert!(catch_unwind(|| log.check_range_index(end + 1)).is_err());
        assert!(catch_unwind(|| log.check_range_index(end + 5)).is_err());
    }

    #[test]
    fn test_check_middle_index() {
        let (start, end, log) = default_log_array();
        assert!(start < end);
        assert!(catch_unwind(|| log.check_middle_index(start - 8)).is_err());
        assert!(catch_unwind(|| log.check_middle_index(start - 1)).is_err());
        assert!(catch_unwind(|| log.check_middle_index(start)).is_err());
        assert!(catch_unwind(|| log.check_middle_index(start + 1)).is_ok());
        assert!(catch_unwind(|| log.check_middle_index(end - 1)).is_ok());
        assert!(catch_unwind(|| log.check_middle_index(end)).is_err());
        assert!(catch_unwind(|| log.check_middle_index(end + 1)).is_err());
        assert!(catch_unwind(|| log.check_middle_index(end + 5)).is_err());
    }

    #[test]
    fn test_check_one_element() {
        let log = make_log_array(0);
        assert!(catch_unwind(|| log.check_one_element()).is_err());
    }

    #[test]
    fn test_integration_test() {
        let mut log = make_log_array(1);
        log.add_command(Term(3), 19);
        log.push(LogEntry {
            term: Term(3),
            index: 2,
            command: LogEntryEnum::Command(3),
        });
        log.add_command(Term(4), 20);
        log.push(LogEntry {
            term: Term(4),
            index: 4,
            command: LogEntryEnum::Command(7),
        });

        for i in 0..100 {
            log.add_command(Term(5), i);
        }
        assert_eq!(0, log.start());
        assert_eq!(105, log.end());

        assert_eq!((0, Term(0)), log.first_index_term().into());
        assert_eq!((104, Term(5)), log.last_index_term().into());

        assert_eq!(8, log.at(8).index);
        assert_eq!(5, log[8].term.0);
        assert_eq!(LogEntryEnum::Command(7), log[4].command);

        log.truncate(50);
        // End changed, start does not.
        assert_eq!(0, log.start());
        assert_eq!(50, log.end());

        assert_eq!((49, Term(5)), log.last_index_term().into());
        assert_eq!(49, log.at(49).index);
        assert_eq!(LogEntryEnum::Command(44), log[49].command);
        assert_eq!(5, log.at(5).term.0);
        // Cannot assert 50 is out of range. log is mut and cannot be used in
        // catch_unwind().

        // Snapshot is not changed.
        assert_eq!(((0, Term(0)).into(), [1, 2, 3].as_ref()), log.snapshot());

        log.shift(5, vec![]);
        // Start changed, end did not;
        assert_eq!(5, log.start());

        assert_eq!((5, Term(5)), log.first_index_term().into());
        assert_eq!(5, log.at(5).index);
        assert_eq!(5, log.at(5).term.0);
        // Cannot assert 4 is out of range. log is mut and cannot be used in
        // catch_unwind().

        // Snapshot is changed, too.
        assert_eq!(((5, Term(5)).into(), [].as_ref()), log.snapshot());

        // Ranged accessors.
        assert_eq!(45, log.all().len());
        assert_eq!(10, log.after(40).len());
        assert_eq!(20, log.between(20, 40).len());

        // Reset!
        log.reset(9, Term(7), vec![7, 8, 9]);
        assert_eq!(10, log.end());
        assert_eq!(1, log.all().len());
        assert_eq!(log.first_index_term(), log.last_index_term());
        assert_eq!(((9, Term(7)).into(), [7, 8, 9].as_ref()), log.snapshot());
    }

    #[test]
    fn test_validate_or_panic_current_term() {
        let log_array = make_log_array(7);
        let last_term = log_array.last_index_term().term.0;
        log_array
            .validate(Term(last_term))
            .expect("Validation should not fail");
        log_array
            .validate(Term(last_term + 1))
            .expect("Validation should not fail");

        let err = log_array
            .validate(Term(last_term - 1))
            .expect_err("Validation should have failed");
        assert!(matches!(
            err,
            ValidationError::FutureTerm(Term(_last_term), _, _)
        ));
    }

    #[test]
    fn test_validate_or_panic_increasing_term() {
        let mut log_array = make_log_array(7);
        let last_term = log_array.last_index_term().term.0;
        log_array
            .validate(Term(last_term + 1))
            .expect("Validation should not fail");
        log_array.inner[1].term = Term(last_term + 1);
        let err = log_array
            .validate(Term(last_term + 1))
            .expect_err("Validation should have failed");
        assert!(matches!(err, ValidationError::TermSpike(2, _)));
    }

    #[test]
    fn test_validate_or_panic_increasing_index() {
        let mut log_array = make_log_array_range(7, 10);
        let last_term = log_array.last_index_term().term.0;
        // OK
        log_array.inner[1].index = 8;
        log_array
            .validate(Term(last_term + 1))
            .expect("Validation should not fail");

        // Not 8, error
        log_array.inner[1].index = 7;
        let err = log_array
            .validate(Term(last_term + 1))
            .expect_err("Validation should have failed");
        assert!(matches!(err, ValidationError::IndexMismatch(8, _)));

        // Not 8, error
        log_array.inner[1].index = 9;
        let err = log_array
            .validate(Term(last_term + 1))
            .expect_err("Validation should have failed");
        assert!(matches!(err, ValidationError::IndexMismatch(8, _)));
    }

    #[test]
    fn test_log_entry_command() {
        let entry = LogEntry::<i32> {
            index: 0,
            term: Term(0),
            command: LogEntryEnum::TermChange,
        };
        assert_eq!(None, entry.command());

        let entry = LogEntry::<i32> {
            index: 0,
            term: Term(0),
            command: LogEntryEnum::Noop,
        };
        assert_eq!(None, entry.command());

        let entry = LogEntry::<i32> {
            index: 0,
            term: Term(0),
            command: LogEntryEnum::Command(1),
        };
        assert_eq!(Some(1), entry.command().cloned());
    }
}

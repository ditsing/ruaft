use crate::{Command, Index, LogEntry, Term};
use std::mem::swap;

pub(crate) struct LogArray {
    inner: Vec<LogEntry>,
    snapshot: bytes::Bytes,
}

impl LogArray {
    pub fn create() -> LogArray {
        let ret = LogArray {
            inner: vec![Self::build_first_entry(0, Term(0))],
            snapshot: bytes::Bytes::new(),
        };
        ret.check_one_element();
        ret
    }

    pub fn restore(inner: Vec<LogEntry>) -> std::io::Result<Self> {
        Ok(LogArray {
            inner,
            snapshot: bytes::Bytes::new(),
        })
    }
}

// Log accessors
impl LogArray {
    pub fn start_offset(&self) -> Index {
        self.first_entry().index
    }

    pub fn len(&self) -> usize {
        self.start_offset() + self.inner.len()
    }

    #[allow(dead_code)]
    pub fn first_index_term(&self) -> (Index, Term) {
        let first_entry = self.first_entry();
        (first_entry.index, first_entry.term)
    }

    pub fn last_index_term(&self) -> (Index, Term) {
        let last_entry = self.last_entry();
        (last_entry.index, last_entry.term)
    }

    pub fn at(&self, index: Index) -> &LogEntry {
        let index = self.check_start_index(index);
        &self.inner[index]
    }

    pub fn after(&self, index: Index) -> &[LogEntry] {
        let index = self.check_start_index(index);
        &self.inner[index..]
    }

    pub fn between(&self, start: Index, end: Index) -> &[LogEntry] {
        let start = self.check_start_index(start);
        let end = self.check_end_index(end);
        &self.inner[start..end]
    }

    pub fn all(&self) -> &[LogEntry] {
        &self.inner[..]
    }

    #[allow(dead_code)]
    pub fn snapshot(&self) -> &bytes::Bytes {
        &self.snapshot
    }
}

impl std::ops::Index<usize> for LogArray {
    type Output = LogEntry;

    fn index(&self, index: usize) -> &Self::Output {
        self.at(index)
    }
}

// Mutations
impl LogArray {
    pub fn add(&mut self, term: Term, command: Command) -> Index {
        let index = self.len();
        self.push(LogEntry {
            index,
            term,
            command,
        });
        index
    }

    pub fn push(&mut self, log_entry: LogEntry) {
        let index = log_entry.index;
        assert_eq!(
            index,
            self.len(),
            "Expecting new index to be exact at len",
        );
        self.inner.push(log_entry);
        assert_eq!(
            index + 1,
            self.len(),
            "Expecting len increase by one after push",
        );
        assert_eq!(
            self.at(index).index,
            index,
            "Expecting pushed element to have the same index",
        );
        self.check_one_element();
    }

    pub fn truncate(&mut self, index: Index) {
        let index = self.check_middle_index(index);
        self.inner.truncate(index);
        self.check_one_element()
    }

    #[allow(dead_code)]
    pub fn shift(&mut self, index: Index, snapshot: bytes::Bytes) {
        // Discard everything before index and store the snapshot.
        let offset = self.check_middle_index(index);
        // WARNING: Potentially all entries after offset would be copied.
        self.inner.drain(0..offset);
        self.snapshot = snapshot;

        // Override the first entry, we know there is at least one entry. This is not strictly
        // needed. One benefit is that the command can be released after this point.
        let (first_index, first_term) = self.first_index_term();
        self.inner[0] = Self::build_first_entry(first_index, first_term);

        assert_eq!(
            first_index, index,
            "Expecting the first entry to have the same index."
        );

        self.check_one_element()
    }

    #[allow(dead_code)]
    pub fn reset(
        &mut self,
        index: Index,
        term: Term,
        snapshot: bytes::Bytes,
    ) -> Vec<LogEntry> {
        let mut inner = vec![Self::build_first_entry(index, term)];
        swap(&mut inner, &mut self.inner);
        self.snapshot = snapshot;

        self.check_one_element();

        inner
    }
}

impl LogArray {
    fn first_entry(&self) -> &LogEntry {
        self.inner
            .first()
            .expect("There must be at least one element in log")
    }

    fn last_entry(&self) -> &LogEntry {
        &self
            .inner
            .last()
            .expect("There must be at least one entry in log")
    }

    fn offset(&self, index: Index) -> usize {
        index - self.start_offset()
    }

    fn check_start_index(&self, index: Index) -> usize {
        assert!(
            index >= self.start_offset() && index < self.len(),
            "Accessing start log index {} out of range [{}, {})",
            index,
            self.start_offset(),
            self.len()
        );

        self.offset(index)
    }

    fn check_end_index(&self, index: Index) -> usize {
        assert!(
            index > self.start_offset() && index <= self.len(),
            "Accessing end log index {} out of range ({}, {}]",
            index,
            self.start_offset(),
            self.len()
        );

        self.offset(index)
    }

    fn check_middle_index(&self, index: Index) -> usize {
        assert!(
            index > self.start_offset() && index < self.len(),
            "Log index {} out of range ({}, {})",
            index,
            self.start_offset(),
            self.len()
        );

        self.offset(index)
    }

    fn check_one_element(&self) {
        assert!(
            self.inner.len() >= 1,
            "There must be at least one element in log"
        )
    }

    fn build_first_entry(index: Index, term: Term) -> LogEntry {
        LogEntry {
            index,
            term,
            command: Command(0),
        }
    }
}

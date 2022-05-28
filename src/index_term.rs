use serde_derive::{Deserialize, Serialize};

use crate::log_array::LogEntry;
use crate::{Index, Term};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexTerm {
    pub index: Index,
    pub term: Term,
}

impl<C> From<&LogEntry<C>> for IndexTerm {
    fn from(entry: &LogEntry<C>) -> Self {
        Self {
            index: entry.index,
            term: entry.term,
        }
    }
}

impl From<IndexTerm> for (Index, Term) {
    fn from(index_term: IndexTerm) -> Self {
        index_term.unpack()
    }
}

impl From<(Index, Term)> for IndexTerm {
    fn from(index_term: (Index, Term)) -> Self {
        Self::pack(index_term.0, index_term.1)
    }
}

impl IndexTerm {
    pub fn unpack(&self) -> (Index, Term) {
        (self.index, self.term)
    }

    pub fn pack(index: Index, term: Term) -> Self {
        Self { index, term }
    }
}

use serde::{Deserialize, Serialize};

use crate::log_array::LogEntry;
use crate::raft::Peer;

pub(crate) fn encode_log_entry<Command: Serialize>(
    log_entry: &LogEntry<Command>,
) -> Vec<u8> {
    bincode::serialize(log_entry).expect("Serialization should not fail")
}

pub(crate) fn decode_log_entry<'a, Command: Deserialize<'a>>(
    stored: &'a [u8],
) -> LogEntry<Command> {
    bincode::deserialize(&stored).expect("Deserialization should never fail")
}

pub(crate) fn encode_voted_for(voted_for: &Option<Peer>) -> String {
    match voted_for {
        Some(Peer(n)) => n.to_string(),
        None => "".to_owned(),
    }
}

pub(crate) fn decode_voted_for(
    stored: &str,
) -> Result<Option<Peer>, std::num::ParseIntError> {
    if stored.is_empty() {
        return Ok(None);
    }
    stored.parse().map(|v| Some(Peer(v)))
}

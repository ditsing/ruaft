#![cfg(feature = "integration-test")]

use crate::{
    AppendEntriesArgs, AppendEntriesReply, IndexTerm, Peer, RequestVoteArgs,
    RequestVoteReply, Term,
};

pub fn make_request_vote_args(
    term: Term,
    peer_id: usize,
    last_log_index: usize,
    last_log_term: Term,
) -> RequestVoteArgs {
    RequestVoteArgs {
        term,
        candidate_id: Peer(peer_id),
        last_log_index,
        last_log_term,
    }
}

pub fn make_append_entries_args<Command>(
    term: Term,
    leader_id: usize,
    prev_log_index: usize,
    prev_log_term: Term,
    leader_commit: usize,
) -> AppendEntriesArgs<Command> {
    AppendEntriesArgs {
        term,
        leader_id: Peer(leader_id),
        prev_log_index,
        prev_log_term,
        entries: vec![],
        leader_commit,
    }
}

pub fn unpack_request_vote_reply(reply: RequestVoteReply) -> (Term, bool) {
    (reply.term, reply.vote_granted)
}

pub fn unpack_append_entries_args<T>(
    request: AppendEntriesArgs<T>,
) -> Option<IndexTerm> {
    request.entries.last().map(|e| e.into())
}

pub fn unpack_append_entries_reply(reply: AppendEntriesReply) -> (Term, bool) {
    (reply.term, reply.success)
}

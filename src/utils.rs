use std::future::Future;
use std::time::Duration;

pub async fn retry_rpc<'a, Func, Fut, T>(
    max_retry: usize,
    deadline: Duration,
    mut task_gen: Func,
) -> std::io::Result<T>
where
    Fut: Future<Output = std::io::Result<T>> + Send + 'a,
    Func: FnMut(usize) -> Fut,
{
    for i in 0..max_retry {
        if i != 0 {
            tokio::time::sleep(Duration::from_millis((1 << i) * 10)).await;
        }
        // Not timed-out.
        #[allow(clippy::collapsible_match)]
        if let Ok(reply) = tokio::time::timeout(deadline, task_gen(i)).await {
            // And no error
            if let Ok(reply) = reply {
                return Ok(reply);
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("Timed out after {} retries", max_retry),
    ))
}

pub const RPC_DEADLINE: Duration = Duration::from_secs(2);

#[cfg(feature = "integration-test")]
pub mod integration_test {
    use crate::{
        AppendEntriesArgs, AppendEntriesReply, Peer, RequestVoteArgs,
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

    pub fn unpack_append_entries_reply(
        reply: AppendEntriesReply,
    ) -> (Term, bool) {
        (reply.term, reply.success)
    }
}

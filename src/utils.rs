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

/// A `std::sync::mpsc::Sender` that is also `Sync`.
///
/// The builtin `Sender` is not sync, because it uses internal mutability to
/// implement an optimization for non-shared one-shot sending. The queue that
/// backs the sender initially accepts only one item from a single producer.
/// If the sender is cloned, the internal queue turns into a multi-producer
/// multi-shot queue. After that, the internal mutability is never invoked
/// again for the sender. The `Sender` structure becomes essentially immutable
/// and thus, `Sync`.
///
/// This optimization, and the internal mutability is meaningless for the
/// purpose of this crate. `SharedSender` forces the transition into a shared
/// queue, and declares itself `Sync`.
///
/// Note that the same reasoning does not apply to the `Receiver`. There are
/// more levels of mutability in the `Receiver`.
#[derive(Clone, Debug)]
pub struct SharedSender<T>(std::sync::mpsc::Sender<T>);

unsafe impl<T> Sync for SharedSender<T> where T: Sync {}
// A better way to implement this might be the following.
//
// unsafe impl<T> Sync for SharedSender<T> where
//    std::sync::mpsc::Flavor<T>::Shared: Sync {}

impl<T> SharedSender<T> {
    /// Create a shared sender.
    pub fn new(inner: std::sync::mpsc::Sender<T>) -> SharedSender<T> {
        // Force the transition to a shared queue in Sender.
        let _clone = inner.clone();
        SharedSender(inner)
    }

    /// A proxy to `std::syc::mpsc::Sender::send()`.
    pub fn send(&self, t: T) -> Result<(), std::sync::mpsc::SendError<T>> {
        self.0.send(t)
    }
}

impl<T> From<std::sync::mpsc::Sender<T>> for SharedSender<T> {
    fn from(inner: std::sync::mpsc::Sender<T>) -> Self {
        Self::new(inner)
    }
}
impl<T> From<SharedSender<T>> for std::sync::mpsc::Sender<T> {
    fn from(this: SharedSender<T>) -> Self {
        this.0
    }
}

lazy_static::lazy_static! {
    static ref THREAD_POOLS: parking_lot::Mutex<std::collections::HashMap<u64, tokio::runtime::Runtime>> =
        parking_lot::Mutex::new(std::collections::HashMap::new());
}

#[derive(Clone)]
pub(crate) struct ThreadPoolHolder {
    id: u64,
    handle: tokio::runtime::Handle,
}

impl ThreadPoolHolder {
    pub fn new(runtime: tokio::runtime::Runtime) -> Self {
        let handle = runtime.handle().clone();
        loop {
            let id: u64 = rand::random();
            if let std::collections::hash_map::Entry::Vacant(v) =
                THREAD_POOLS.lock().entry(id)
            {
                v.insert(runtime);
                break Self { id, handle };
            }
        }
    }

    pub fn take(self) -> Option<tokio::runtime::Runtime> {
        THREAD_POOLS.lock().remove(&self.id)
    }
}

impl std::ops::Deref for ThreadPoolHolder {
    type Target = tokio::runtime::Handle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

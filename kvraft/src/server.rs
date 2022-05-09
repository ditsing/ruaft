use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver};
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

use crate::{CommitSentinelArgs, CommitSentinelReply};
use ruaft::{
    ApplyCommandMessage, Index, Persister, Raft, RemoteRaft, Term,
    VerifyAuthorityResult,
};
use test_utils::log_with;
use test_utils::thread_local_logger::LocalLogger;

use crate::common::{
    ClerkId, GetArgs, GetReply, KVError, PutAppendArgs, PutAppendEnum,
    PutAppendReply, UniqueId,
};
use crate::snapshot_holder::SnapshotHolder;

pub struct KVServer {
    me: usize,
    state: Mutex<KVServerState>,
    rf: Raft<UniqueKVOp>,
    keep_running: AtomicBool,
    logger: LocalLogger,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UniqueKVOp {
    op: KVOp,
    me: usize,
    unique_id: UniqueId,
}

#[derive(Default, Serialize, Deserialize)]
struct KVServerState {
    raft_index: Index,
    kv: HashMap<String, String>,
    debug_kv: HashMap<String, String>,
    applied_op: HashMap<ClerkId, (UniqueId, CommitResult)>,
    #[allow(clippy::type_complexity)]
    #[serde(skip)]
    queries: HashMap<
        UniqueId,
        (
            Arc<ResultHolder>,
            futures::channel::oneshot::Sender<
                Result<CommitResult, CommitError>,
            >,
        ),
    >,
    #[serde(skip)]
    index_subscribers: HashMap<
        Index,
        Vec<(String, futures::channel::oneshot::Sender<Option<String>>)>,
    >,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum KVOp {
    NoOp,
    Get(String),
    Put(String, String),
    Append(String, String),
}

impl Default for KVOp {
    fn default() -> Self {
        KVOp::NoOp
    }
}

struct ResultHolder {
    term: AtomicUsize,
    peeks: AtomicUsize,
    result: futures::future::Shared<
        futures::channel::oneshot::Receiver<Result<CommitResult, CommitError>>,
    >,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum CommitResult {
    Get(Option<String>),
    Put,
    Append,
}

#[derive(Clone, Debug)]
enum CommitError {
    NotLeader,
    Expired(UniqueId),
    TimedOut,
    #[allow(dead_code)]
    Conflict,
    NotMe(CommitResult),
    Duplicate(CommitResult),
}

impl From<CommitError> for KVError {
    fn from(err: CommitError) -> Self {
        match err {
            CommitError::NotLeader => KVError::NotLeader,
            CommitError::Expired(_) => KVError::Expired,
            CommitError::TimedOut => KVError::TimedOut,
            CommitError::Conflict => KVError::Conflict,
            CommitError::NotMe(_) => panic!("NotMe is not a KVError"),
            CommitError::Duplicate(_) => panic!("Duplicate is not a KVError"),
        }
    }
}

impl KVServer {
    pub fn new(
        servers: Vec<impl RemoteRaft<UniqueKVOp> + 'static>,
        me: usize,
        persister: Arc<dyn Persister>,
        max_state_size_bytes: Option<usize>,
    ) -> Arc<Self> {
        let (tx, rx) = channel();
        let apply_command = move |message| {
            // Ignore apply errors.
            let _ = tx.send(message);
        };
        let snapshot_holder = Arc::new(SnapshotHolder::default());
        let snapshot_holder_clone = snapshot_holder.clone();
        let ret = Arc::new(Self {
            me,
            state: Default::default(),
            rf: Raft::new(
                servers,
                me,
                persister,
                apply_command,
                max_state_size_bytes,
                move |index| snapshot_holder_clone.request_snapshot(index),
            ),
            keep_running: AtomicBool::new(true),
            logger: LocalLogger::inherit(),
        });
        ret.process_command(snapshot_holder, rx);
        ret
    }

    fn apply_op(&self, unique_id: UniqueId, leader: usize, op: KVOp) {
        // The borrow checker does not allow borrowing two fields of an instance
        // inside a MutexGuard. But it does allow borrowing two fields of the
        // instance itself. Calling deref_mut() on the MutexGuard works, too!
        let state = &mut *self.state.lock();
        let (applied_op, kv) = (&mut state.applied_op, &mut state.kv);
        let entry = applied_op.entry(unique_id.clerk_id);
        if let Entry::Occupied(curr) = &entry {
            let (applied_unique_id, _) = curr.get();
            if *applied_unique_id >= unique_id {
                // Redelivered.
                // It is guaranteed that we have no pending queries with the
                // same unique_id, because
                // 1. When inserting into queries, we first check the unique_id
                // is strictly larger than the one in applied_op.
                // 2. When modifying entries in applied_op, the unique_id can
                // only grow larger. And we make sure there is no entries with
                // the same unique_id in queries.
                // TODO(ditsing): in case 2), make sure there is no entries in
                // queries that have a smaller unique_id.
                assert!(!state.queries.contains_key(&unique_id));
                return;
            }
        }

        let result = match op {
            KVOp::NoOp => return,
            KVOp::Get(key) => CommitResult::Get(kv.get(&key).cloned()),
            KVOp::Put(key, value) => {
                kv.insert(key, value);
                CommitResult::Put
            }
            KVOp::Append(key, value) => {
                kv.entry(key)
                    .and_modify(|str| str.push_str(&value))
                    .or_insert(value);
                CommitResult::Append
            }
        };

        match entry {
            Entry::Occupied(mut curr) => {
                curr.insert((unique_id, result.clone()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert((unique_id, result.clone()));
            }
        }

        if let Some((_, sender)) = state.queries.remove(&unique_id) {
            // This KV server might not be the same leader that committed the
            // query. We are not sure if it is a duplicate or a conflict. To
            // tell the difference, terms of all queries must be stored.
            let _ = sender.send(if leader == self.me {
                Ok(result)
            } else {
                Err(CommitError::NotMe(result))
            });
        };
    }

    fn process_read_requests(&self, index: Index) {
        let mut state = self.state.lock();
        assert!(index > state.raft_index);
        for index in state.raft_index..=index {
            if let Some(read_requests) = state.index_subscribers.remove(&index)
            {
                for (key, sender) in read_requests {
                    let _ = sender.send(state.kv.get(&key).cloned());
                }
            }
        }
        state.raft_index = index;
    }

    fn restore_state(&self, mut new_state: KVServerState) {
        let mut state = self.state.lock();
        // Cleanup all existing queries.
        for (_, (_, sender)) in state.queries.drain() {
            let _ = sender.send(Err(CommitError::NotLeader));
        }

        std::mem::swap(&mut new_state, &mut *state);
    }

    fn process_command(
        self: &Arc<Self>,
        snapshot_holder: Arc<SnapshotHolder<KVServerState>>,
        command_channel: Receiver<ApplyCommandMessage<UniqueKVOp>>,
    ) {
        let this = Arc::downgrade(self);
        let logger = LocalLogger::inherit();
        let me = self.me;
        std::thread::spawn(move || {
            logger.attach();
            log::info!("KVServer {} waiting for commands ...", me);
            while let Ok(message) = command_channel.recv() {
                if let Some(this) = this.upgrade() {
                    match message {
                        ApplyCommandMessage::Snapshot(snapshot) => {
                            let state = snapshot_holder.load_snapshot(snapshot);
                            this.restore_state(state);
                        }
                        ApplyCommandMessage::Command(index, command) => {
                            this.apply_op(
                                command.unique_id,
                                command.me,
                                command.op,
                            );
                            this.process_read_requests(index);
                            if let Some(snapshot) = snapshot_holder
                                .take_snapshot(&this.state.lock(), index)
                            {
                                this.rf.save_snapshot(snapshot);
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            log::info!("KVServer {} stopped waiting for commands.", me);
        });
    }

    async fn block_for_read(
        &self,
        key: String,
    ) -> Result<Option<String>, KVError> {
        let result_fut = match self.rf.verify_authority_async() {
            Some(result_fut) => result_fut,
            None => return Err(KVError::NotLeader),
        };
        let index =
            match tokio::time::timeout(Self::DEFAULT_TIMEOUT, result_fut).await
            {
                Ok(VerifyAuthorityResult::Success(index)) => index,
                Ok(VerifyAuthorityResult::TermElapsed) => {
                    return Err(KVError::NotLeader)
                }
                Ok(VerifyAuthorityResult::TimedOut) => {
                    return Err(KVError::TimedOut)
                }
                Err(_e) => return Err(KVError::TimedOut),
            };
        let receiver = {
            let state = self.state.lock();
            if state.raft_index >= index {
                return Ok(state.kv.get(&key).cloned());
            }
            let (sender, receiver) = futures::channel::oneshot::channel();
            // The mutex guard is moved into this scope and dropped here.
            let mut state = state;
            let queue = state.index_subscribers.entry(index).or_default();
            queue.push((key, sender));
            receiver
        };

        receiver.await.map_err(|_e| KVError::TimedOut)
    }

    const UNSEEN_TERM: usize = 0;
    const ATTEMPTING_TERM: usize = usize::MAX;
    async fn block_for_commit(
        &self,
        unique_id: UniqueId,
        op: KVOp,
        timeout: Duration,
    ) -> Result<CommitResult, CommitError> {
        if !self.keep_running.load(Ordering::SeqCst) {
            return Err(CommitError::NotLeader);
        }
        let result_holder = {
            let mut state = self.state.lock();
            let applied = state.applied_op.get(&unique_id.clerk_id);
            if let Some((applied_unique_id, result)) = applied {
                #[allow(clippy::comparison_chain)]
                if unique_id < *applied_unique_id {
                    return Err(CommitError::Expired(unique_id));
                } else if unique_id == *applied_unique_id {
                    return Err(CommitError::Duplicate(result.clone()));
                }
            };
            let entry = state.queries.entry(unique_id).or_insert_with(|| {
                let (tx, rx) = futures::channel::oneshot::channel();
                (
                    Arc::new(ResultHolder {
                        term: AtomicUsize::new(Self::UNSEEN_TERM),
                        peeks: AtomicUsize::new(0),
                        result: rx.shared(),
                    }),
                    tx,
                )
            });
            entry.0.clone()
        };

        let (Term(hold_term), is_leader) = self.rf.get_state();
        if !is_leader {
            return Err(CommitError::NotLeader);
        }
        Self::validate_term(hold_term);

        let set = result_holder.term.compare_exchange(
            Self::UNSEEN_TERM,
            Self::ATTEMPTING_TERM,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        let start = match set {
            // Nobody has attempted start() yet.
            Ok(Self::UNSEEN_TERM) => true,
            Ok(_) => panic!(
                "compare_exchange should always return the current value 0"
            ),
            // Somebody is attempting start().
            Err(Self::ATTEMPTING_TERM) => false,
            // Somebody has attempted start().
            Err(prev_term) if prev_term < hold_term => {
                let set = result_holder.term.compare_exchange(
                    prev_term,
                    Self::ATTEMPTING_TERM,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                set.is_ok()
            }
            _ => false,
        };
        if start {
            let op = UniqueKVOp {
                op,
                me: self.me,
                unique_id,
            };
            let start = log_with!(self.logger, self.rf.start(op));
            let start_term =
                start.map_or(Self::UNSEEN_TERM, |(Term(term), _)| {
                    Self::validate_term(term);
                    term
                });
            let set = result_holder.term.compare_exchange(
                Self::ATTEMPTING_TERM,
                start_term,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            // Setting term must have been successful, and must return the
            // value previously set by this attempt.
            assert_eq!(set, Ok(Self::ATTEMPTING_TERM));

            if start_term == Self::UNSEEN_TERM {
                return Err(CommitError::NotLeader);
            }
        }

        let result = result_holder.result.clone();
        // Wait for the op to be committed.
        let result = tokio::time::timeout(timeout, result).await;
        match result {
            Ok(Ok(Ok(result))) => {
                // If the result is OK, all other requests should see "Duplicate".
                if result_holder.peeks.fetch_add(1, Ordering::Relaxed) == 0 {
                    Ok(result)
                } else {
                    Err(CommitError::Duplicate(result))
                }
            }
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => Err(CommitError::NotLeader),
            Err(_) => Err(CommitError::TimedOut),
        }
    }

    fn validate_term(term: usize) {
        assert!(term > Self::UNSEEN_TERM, "Term must be larger than 0.");
        assert!(
            term < Self::ATTEMPTING_TERM,
            "Term must be smaller than usize::MAX."
        );
    }

    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);

    pub async fn commit_sentinel(
        &self,
        args: CommitSentinelArgs,
    ) -> CommitSentinelReply {
        let result_fut = self.block_for_commit(
            args.unique_id,
            KVOp::Get(String::new()),
            Self::DEFAULT_TIMEOUT,
        );
        let result = match result_fut.await {
            Ok(CommitResult::Get(_)) => Ok(()),
            Ok(CommitResult::Put) => Err(KVError::Conflict),
            Ok(CommitResult::Append) => Err(KVError::Conflict),
            Err(CommitError::Duplicate(_)) => Err(KVError::Conflict),
            Err(CommitError::NotMe(_)) => Err(KVError::Conflict),
            Err(e) => Err(e.into()),
        };
        CommitSentinelReply { result }
    }

    pub async fn get(&self, args: GetArgs) -> GetReply {
        let result = self.block_for_read(args.key).await;
        GetReply { result }
    }

    pub async fn put_append(&self, args: PutAppendArgs) -> PutAppendReply {
        let op = match args.op {
            PutAppendEnum::Put => KVOp::Put(args.key, args.value),
            PutAppendEnum::Append => KVOp::Append(args.key, args.value),
        };
        let result_fut =
            self.block_for_commit(args.unique_id, op, Self::DEFAULT_TIMEOUT);
        let result = match result_fut.await {
            Ok(result) => result,
            Err(CommitError::Duplicate(result)) => result,
            Err(CommitError::NotMe(result)) => result,
            Err(e) => {
                return PutAppendReply {
                    result: Err(e.into()),
                }
            }
        };
        let result = match result {
            CommitResult::Put => {
                if args.op == PutAppendEnum::Put {
                    Ok(())
                } else {
                    Err(KVError::Conflict)
                }
            }
            CommitResult::Append => {
                if args.op == PutAppendEnum::Append {
                    Ok(())
                } else {
                    Err(KVError::Conflict)
                }
            }
            CommitResult::Get(_) => Err(KVError::Conflict),
        };

        PutAppendReply { result }
    }

    pub fn raft(&self) -> &Raft<UniqueKVOp> {
        &self.rf
    }

    pub fn kill(self: Arc<Self>) {
        // Return error to new queries.
        self.keep_running.store(false, Ordering::SeqCst);
        // Cancel all in-flight queries.
        for (_, (_, sender)) in self.state.lock().queries.drain() {
            let _ = sender.send(Err(CommitError::NotLeader));
        }

        let rf = self.raft().clone();
        // We must drop self to remove the only clone of raft, so that
        // `rf.kill()` does not block.
        drop(self);
        rf.kill();
        // The process_command thread will exit, after Raft drops the reference
        // to the sender.
    }
}

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use serde_derive::{Deserialize, Serialize};

use ruaft::{ApplyCommandMessage, Persister, Raft, RemoteRaft, Term};
use test_utils::thread_local_logger::LocalLogger;

use crate::common::{
    ClerkId, GetArgs, GetEnum, GetReply, KVError, PutAppendArgs, PutAppendEnum,
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

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct UniqueKVOp {
    op: KVOp,
    me: usize,
    unique_id: UniqueId,
}

#[derive(Default, Serialize, Deserialize)]
struct KVServerState {
    kv: HashMap<String, String>,
    debug_kv: HashMap<String, String>,
    applied_op: HashMap<ClerkId, (UniqueId, CommitResult)>,
    #[serde(skip)]
    queries: HashMap<UniqueId, Arc<ResultHolder>>,
}

#[derive(Clone, Serialize, Deserialize)]
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
    result: Mutex<Result<CommitResult, CommitError>>,
    condvar: Condvar,
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

        if let Some(result_holder) = state.queries.remove(&unique_id) {
            // This KV server might not be the same leader that committed the
            // query. We are not sure if it is a duplicate or a conflict. To
            // tell the difference, terms of all queries must be stored.
            *result_holder.result.lock() = if leader == self.me {
                Ok(result)
            } else {
                Err(CommitError::NotMe(result))
            };
            result_holder.condvar.notify_all();
        };
    }

    fn restore_state(&self, mut new_state: KVServerState) {
        let mut state = self.state.lock();
        // Cleanup all existing queries.
        for result_holder in state.queries.values() {
            *result_holder.result.lock() = Err(CommitError::NotLeader);
            result_holder.condvar.notify_all();
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

    const UNSEEN_TERM: usize = 0;
    const ATTEMPTING_TERM: usize = usize::MAX;
    fn block_for_commit(
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
                Arc::new(ResultHolder {
                    term: AtomicUsize::new(Self::UNSEEN_TERM),
                    result: Mutex::new(Err(CommitError::TimedOut)),
                    condvar: Condvar::new(),
                })
            });
            entry.clone()
        };

        let (Term(hold_term), is_leader) = self.rf.get_state();
        if !is_leader {
            result_holder.condvar.notify_all();
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
            let start = self.rf.start(op);
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
                result_holder.condvar.notify_all();
                return Err(CommitError::NotLeader);
            }
        }

        let mut guard = result_holder.result.lock();
        // Wait for the op to be committed.
        result_holder.condvar.wait_for(&mut guard, timeout);

        // Copy the result out.
        let result = guard.clone();
        // If the result is OK, all other requests should see "Duplicate".
        if let Ok(result) = guard.clone() {
            *guard = Err(CommitError::Duplicate(result))
        }

        result
    }

    fn validate_term(term: usize) {
        assert!(term > Self::UNSEEN_TERM, "Term must be larger than 0.");
        assert!(
            term < Self::ATTEMPTING_TERM,
            "Term must be smaller than usize::MAX."
        );
    }

    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);

    pub fn get(&self, args: GetArgs) -> GetReply {
        self.logger.clone().attach();
        let map_dup = match args.op {
            GetEnum::AllowDuplicate => |r| Ok(r),
            GetEnum::NoDuplicate => |_| Err(KVError::Conflict),
        };
        let result = match self.block_for_commit(
            args.unique_id,
            KVOp::Get(args.key),
            Self::DEFAULT_TIMEOUT,
        ) {
            Ok(result) => Ok(result),
            Err(CommitError::Duplicate(result)) => map_dup(result),
            Err(CommitError::NotMe(result)) => map_dup(result),
            Err(e) => Err(e.into()),
        };
        let result = match result {
            Ok(result) => result,
            Err(e) => return GetReply { result: Err(e) },
        };
        let result = match result {
            CommitResult::Get(result) => Ok(result),
            CommitResult::Put => Err(KVError::Conflict),
            CommitResult::Append => Err(KVError::Conflict),
        };
        GetReply { result }
    }

    pub fn put_append(&self, args: PutAppendArgs) -> PutAppendReply {
        self.logger.clone().attach();
        let op = match args.op {
            PutAppendEnum::Put => KVOp::Put(args.key, args.value),
            PutAppendEnum::Append => KVOp::Append(args.key, args.value),
        };
        let result = match self.block_for_commit(
            args.unique_id,
            op,
            Self::DEFAULT_TIMEOUT,
        ) {
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
        for result_holder in self.state.lock().queries.values() {
            *result_holder.result.lock() = Err(CommitError::NotLeader);
            result_holder.condvar.notify_all();
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

use super::common::{
    ClerkId, GetArgs, GetReply, KVError, PutAppendArgs, PutAppendEnum,
    PutAppendReply, UniqueId,
};
use parking_lot::{Condvar, Mutex};
use ruaft::{Persister, Raft, RpcClient};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver};
use std::sync::Arc;
use std::time::Duration;

pub struct KVServer {
    state: Mutex<KVServerState>,
    rf: Mutex<Raft<UniqueKVOp>>,
    // snapshot
}

type IndexedCommand = (usize, UniqueKVOp);

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct UniqueKVOp {
    op: KVOp,
    unique_id: UniqueId,
}

#[derive(Default)]
struct KVServerState {
    me: usize,
    kv: HashMap<String, String>,
    debug_kv: HashMap<String, String>,
    applied_op: HashMap<ClerkId, (UniqueId, CommitResult)>,
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
    result: Mutex<Result<CommitResult, CommitError>>,
    condvar: Condvar,
}

#[derive(Clone, Debug)]
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
    Conflict,
    Duplicate(CommitResult),
}

impl From<CommitError> for KVError {
    fn from(err: CommitError) -> Self {
        match err {
            CommitError::NotLeader => KVError::NotLeader,
            CommitError::Expired(_) => KVError::Expired,
            CommitError::TimedOut => KVError::TimedOut,
            CommitError::Conflict => KVError::Conflict,
            CommitError::Duplicate(_) => panic!("Duplicate is not a KVError"),
        }
    }
}

impl KVServer {
    pub fn new(
        servers: Vec<RpcClient>,
        me: usize,
        persister: Arc<dyn Persister>,
    ) -> Arc<Self> {
        let (tx, rx) = channel();
        let apply_command = move |index, command| {
            tx.send((index, command))
                .expect("The receiving end of apply command channel should have not been dropped");
        };
        let ret = Arc::new(Self {
            state: Mutex::new(KVServerState {
                me,
                ..Default::default()
            }),
            rf: Mutex::new(Raft::new(
                servers,
                me,
                persister,
                apply_command,
                None,
                Raft::<UniqueKVOp>::NO_SNAPSHOT,
            )),
        });
        ret.clone().process_command(rx);
        ret
    }

    fn apply_op(&self, unique_id: UniqueId, op: KVOp) {
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
            *result_holder.result.lock() = Ok(result);
            result_holder.condvar.notify_all();
        };
    }

    fn process_command(
        self: Arc<Self>,
        command_channel: Receiver<IndexedCommand>,
    ) {
        std::thread::spawn(move || {
            while let Ok((_, command)) = command_channel.recv() {
                self.apply_op(command.unique_id, command.op);
            }
        });
    }

    fn block_for_commit(
        &self,
        unique_id: UniqueId,
        op: KVOp,
        timeout: Duration,
    ) -> Result<CommitResult, CommitError> {
        let result_holder = {
            let mut state = self.state.lock();
            let applied = state.applied_op.get(&unique_id.clerk_id);
            if let Some((applied_unique_id, result)) = applied {
                if unique_id < *applied_unique_id {
                    return Err(CommitError::Expired(unique_id));
                } else if unique_id == *applied_unique_id {
                    return Err(CommitError::Duplicate(result.clone()));
                }
            };
            let entry = state.queries.entry(unique_id).or_insert_with(|| {
                Arc::new(ResultHolder {
                    result: Mutex::new(Err(CommitError::TimedOut)),
                    condvar: Condvar::new(),
                })
            });
            entry.clone()
        };

        let op = UniqueKVOp { op, unique_id };
        if self.rf.lock().start(op).is_none() {
            return Err(CommitError::NotLeader);
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

        return result;
    }

    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

    pub fn get(&self, args: GetArgs) -> GetReply {
        let (is_retry, result) = match self.block_for_commit(
            args.unique_id,
            KVOp::Get(args.key),
            Self::DEFAULT_TIMEOUT,
        ) {
            Ok(result) => (false, result),
            Err(CommitError::Duplicate(result)) => (true, result),
            Err(e) => {
                return GetReply {
                    result: Err(e.into()),
                    is_retry: false,
                }
            }
        };
        let result = match result {
            CommitResult::Get(result) => Ok(result),
            CommitResult::Put => Err(KVError::Conflict),
            CommitResult::Append => Err(KVError::Conflict),
        };
        GetReply { result, is_retry }
    }

    pub fn put_append(&self, args: PutAppendArgs) -> PutAppendReply {
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

    pub fn raft(&self) -> Raft<UniqueKVOp> {
        self.rf.lock().clone()
    }

    pub fn kill(self) {
        self.rf.into_inner().kill()
        // The process_command thread will exit, after Raft drops the reference
        // to the sender.
    }
}

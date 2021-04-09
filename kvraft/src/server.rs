use super::common::{
    ClerkId, GetArgs, GetReply, KVError, PutAppendArgs, PutAppendEnum,
    PutAppendReply, UniqueId,
};
use parking_lot::{Condvar, Mutex};
use ruaft::{Persister, Raft, RpcClient};
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver};
use std::sync::Arc;
use std::time::Duration;

struct KVServer {
    state: Mutex<KVServerState>,
    rf: Mutex<Raft<UniqueKVOp>>,
    // snapshot
}

type IndexedCommand = (usize, UniqueKVOp);

#[derive(Clone, Default, Serialize, Deserialize)]
struct UniqueKVOp {
    op: KVOp,
    unique_id: UniqueId,
}

#[derive(Default)]
struct KVServerState {
    kv: HashMap<String, String>,
    debug_kv: HashMap<String, String>,
    applied_op: HashMap<ClerkId, UniqueKVOpStep>,
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

struct UniqueKVOpStep {
    step: KVOpStep,
    unique_id: UniqueId,
}

enum KVOpStep {
    Unseen,
    Pending(Arc<ResultHolder>),
    Done(CommitResult),
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
            state: Default::default(),
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

    fn find_op_or_unseen(
        applied_op: &mut HashMap<ClerkId, UniqueKVOpStep>,
        unique_id: UniqueId,
    ) -> &mut UniqueKVOpStep {
        applied_op
            .entry(unique_id.clerk_id)
            .and_modify(|e| {
                if let KVOpStep::Unseen = e.step {
                    panic!("Unseen op should never been here.")
                }
            })
            .or_insert_with(|| UniqueKVOpStep {
                step: KVOpStep::Unseen,
                unique_id,
            })
    }

    fn apply_op(&self, unique_id: UniqueId, op: KVOp) {
        // The borrow checker does not allow borrowing two fields of an instance
        // inside a MutexGuard. But it does allow borrowing two fields of the
        // instance itself. Calling deref_mut() on the MutexGuard works, too!
        let state = &mut *self.state.lock();
        let (applied_op, kv) = (&mut state.applied_op, &mut state.kv);
        let curr_op = Self::find_op_or_unseen(applied_op, unique_id);
        if unique_id < curr_op.unique_id {
            // Redelivered.
            return;
        }
        if unique_id == curr_op.unique_id {
            if let KVOpStep::Done(_) = curr_op.step {
                // Redelivered.
                return;
            }
        }
        assert!(unique_id >= curr_op.unique_id);

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
        let last_op = std::mem::replace(
            curr_op,
            UniqueKVOpStep {
                step: KVOpStep::Done(result.clone()),
                unique_id,
            },
        );
        assert!(unique_id >= last_op.unique_id);
        if let KVOpStep::Pending(result_holder) = last_op.step {
            *result_holder.result.lock() = if unique_id == last_op.unique_id {
                Ok(result)
            } else {
                Err(CommitError::Expired(last_op.unique_id))
            };
            result_holder.condvar.notify_all();
        }
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
        let (unseen, result_holder) = {
            let mut state = self.state.lock();
            let last_result =
                Self::find_op_or_unseen(&mut state.applied_op, unique_id);

            // We know that the two unique_ids must come from the same clerk,
            // because they are found in the same entry of applied_op.
            assert_eq!(unique_id.clerk_id, last_result.unique_id.clerk_id);

            // This is a newer request
            if unique_id > last_result.unique_id {
                last_result.unique_id = unique_id;
                match &last_result.step {
                    KVOpStep::Unseen => {
                        panic!("Unseen results should never be seen.")
                    }
                    // Notify all threads that are still waiting that a new
                    // request has arrived. This should never happen.
                    KVOpStep::Pending(result_holder) => {
                        result_holder.condvar.notify_all();
                    }
                    KVOpStep::Done(_) => {}
                }
                last_result.step = KVOpStep::Unseen;
            }

            // Now we know unique_id <= last_result.unique_id.
            assert!(unique_id <= last_result.unique_id);
            match &last_result.step {
                KVOpStep::Unseen => {
                    let result_holder = Arc::new(ResultHolder {
                        // The default error is timed-out, if no one touches the
                        // result holder at all.
                        result: Mutex::new(Err(CommitError::TimedOut)),
                        condvar: Condvar::new(),
                    });
                    last_result.step = KVOpStep::Pending(result_holder.clone());
                    (true, result_holder)
                }
                // The operation is still pending.
                KVOpStep::Pending(result_holder) => {
                    (false, result_holder.clone())
                }
                // The operation is a Get
                KVOpStep::Done(CommitResult::Get(value)) => {
                    return if unique_id == last_result.unique_id {
                        // This is the same operation as the last one
                        Ok(CommitResult::Get(value.clone()))
                    } else {
                        // A past Get operation is being retried. We do not
                        // know the proper value to return.
                        Err(CommitError::Expired(unique_id))
                    };
                }
                // For Put & Append operations, all we know is that all past
                // operations must have been committed, returning OK.
                KVOpStep::Done(result) => return Ok(result.clone()),
            }
        };
        if unseen {
            let op = UniqueKVOp { op, unique_id };
            if self.rf.lock().start(op).is_none() {
                return Err(CommitError::NotLeader);
            }
        }
        let mut result = result_holder.result.lock();
        // Wait for the op to be committed.
        result_holder.condvar.wait_for(&mut result, timeout);
        let result = result.clone();
        return if let Ok(result) = result {
            if unseen {
                Ok(result)
            } else {
                Err(CommitError::Duplicate(result))
            }
        } else {
            result
        };
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

    pub fn kill(self) {
        self.rf.into_inner().kill()
        // The process_command thread will exit, after Raft drops the reference
        // to the sender.
    }
}

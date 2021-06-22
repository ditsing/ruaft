use super::common::{ClerkId, UniqueId};
use crate::kvraft::common::{
    GetArgs, GetReply, KVError, PutAppendArgs, PutAppendEnum, PutAppendReply,
};
use parking_lot::{Condvar, Mutex};
use ruaft::{Persister, Raft, RpcClient};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver};
use std::sync::Arc;
use std::time::Duration;

struct KVServer {
    state: Mutex<KVServerState>,
    rf: Raft<UniqueKVOp>,
    command_channel: Receiver<(usize, UniqueKVOp)>,
    shutdown: AtomicBool,
    // snapshot
}

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
    Get(GetOp),
    Put(PutAppendOp),
    Append(PutAppendOp),
}

impl Default for KVOp {
    fn default() -> Self {
        KVOp::NoOp
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct GetOp {
    key: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct PutAppendOp {
    key: String,
    value: String,
}

struct UniqueKVOpStep {
    step: KVOpStep,
    unique_id: UniqueId,
}

enum KVOpStep {
    Unseen,
    Pending(Arc<Condvar>),
    Done(CommitResult),
}

#[derive(Clone, Debug)]
enum CommitResult {
    Get(Option<String>),
    Put,
    Append,
}

#[derive(Debug)]
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
    ) -> Self {
        let (tx, rx) = channel();
        let apply_command = move |index, command| {
            tx.send((index, command))
                .expect("The receiving end of apply command channel should have not been dropped");
        };
        Self {
            state: Default::default(),
            rf: Raft::new(
                servers,
                me,
                persister,
                apply_command,
                None,
                Raft::<UniqueKVOp>::NO_SNAPSHOT,
            ),
            command_channel: rx,
            shutdown: AtomicBool::new(false),
        }
    }

    fn block_for_commit(
        &self,
        unique_id: UniqueId,
        op: KVOp,
        timeout: Duration,
    ) -> Result<CommitResult, CommitError> {
        let (unseen, condvar) = {
            let mut state = self.state.lock();
            let last_result = state
                .applied_op
                .entry(unique_id.clerk_id)
                .or_insert_with(|| UniqueKVOpStep {
                    step: KVOpStep::Unseen,
                    unique_id,
                });

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
                    KVOpStep::Pending(condvar) => {
                        condvar.notify_all();
                    }
                    KVOpStep::Done(_) => {}
                }
                last_result.step = KVOpStep::Unseen;
            }

            // Now we know unique_id <= last_result.unique_id.
            assert!(unique_id <= last_result.unique_id);
            match &last_result.step {
                KVOpStep::Unseen => {
                    let condvar = Arc::new(Condvar::new());
                    last_result.step = KVOpStep::Pending(condvar.clone());
                    (true, condvar)
                }
                // The operation is still pending.
                KVOpStep::Pending(condvar) => (false, condvar.clone()),
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
            if self.rf.start(op).is_none() {
                return Err(CommitError::NotLeader);
            }
        }
        let mut state = self.state.lock();
        // Wait for the op to be comitted.
        condvar.wait_for(&mut state, timeout);
        let step = state
            .applied_op
            .get(&unique_id.clerk_id)
            .expect("Clerk entry should have been inserted.");

        if unique_id != step.unique_id {
            // The clerk must have seen the result of this request because they
            // are sending in a new one. Just return error.
            return Err(CommitError::Expired(unique_id));
        }
        return if let KVOpStep::Done(result) = &step.step {
            if unseen {
                Ok(result.clone())
            } else {
                Err(CommitError::Duplicate(result.clone()))
            }
        } else {
            Err(CommitError::TimedOut)
        };
    }

    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

    pub fn get(&self, args: GetArgs) -> GetReply {
        let (is_retry, result) = match self.block_for_commit(
            args.unique_id,
            KVOp::Get(GetOp { key: args.key }),
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
        let op = PutAppendOp {
            key: args.key,
            value: args.value,
        };
        let op = match args.op {
            PutAppendEnum::Put => KVOp::Put(op),
            PutAppendEnum::Append => KVOp::Append(op),
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
        self.shutdown.store(true, Ordering::Relaxed);
        self.rf.kill()
    }
}

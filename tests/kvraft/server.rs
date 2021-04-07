use super::common::UniqueId;
use parking_lot::Mutex;
use ruaft::{Persister, Raft, RpcClient};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{channel, Receiver};
use std::sync::Arc;

struct KVServer {
    state: Mutex<KVServerState>,
    rf: Raft<KVOp>,
    command_channel: Receiver<(usize, KVOp)>,
    shutdown: AtomicBool,
    // snapshot
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct KVOp {
    unique_id: UniqueId,
}

#[derive(Default)]
struct KVServerState {
    kv: HashMap<String, String>,
    debug_kv: HashMap<String, String>,
    applied_op: HashMap<UniqueId, KVOp>,
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
                Raft::<KVOp>::NO_SNAPSHOT,
            ),
            command_channel: rx,
            shutdown: AtomicBool::new(false),
        }
    }
}

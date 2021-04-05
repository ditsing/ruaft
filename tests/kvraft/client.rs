use super::common::{
    call_rpc, GetArgs, GetReply, PutAppendArgs, PutAppendReply,
    UniqueIdSequence, GET, PUT_APPEND,
};
use labrpc::Client;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Once;

struct Clerk {
    servers: Vec<Client>,

    last_server_index: AtomicUsize,
    unique_id: UniqueIdSequence,
    init: Once,

    executor: tokio::runtime::Runtime,
}

impl Clerk {
    pub fn new(servers: Vec<Client>) -> Self {
        Self {
            servers,

            last_server_index: AtomicUsize::new(0),
            unique_id: UniqueIdSequence::new(),
            init: Once::new(),

            executor: tokio::runtime::Builder::new_multi_thread()
                .thread_name("kvraft-clerk")
                .worker_threads(1)
                .build()
                .expect("Creating thread pool should not fail"),
        }
    }

    fn commit_sentinel(&mut self) {
        loop {
            let index = self.server_index();
            let client = &self.servers[index];
            let args = GetArgs {
                key: "".to_string(),
                unique_id: self.unique_id.zero(),
            };
            let reply: labrpc::Result<GetReply> =
                self.executor.block_on(call_rpc(client, GET, args));
            if let Ok(reply) = reply {
                match reply.result {
                    Ok(_) => {
                        if !reply.is_retry {
                            // Discard the used unique_id.
                            self.unique_id.inc();
                            break;
                        } else {
                            // The RPC was successful, but the server has had an
                            // exact same entry, which means someone else has taken
                            // that clerk_id.
                            self.unique_id = UniqueIdSequence::new();
                        }
                    }
                    Err(_) => {}
                };
            };
        }
    }

    fn server_index(&self) -> usize {
        self.last_server_index.load(Ordering::Relaxed)
    }
}

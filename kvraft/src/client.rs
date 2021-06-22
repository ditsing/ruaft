use super::common::{
    GetArgs, GetReply, KVRaftOptions, PutAppendArgs, PutAppendEnum,
    PutAppendReply, UniqueIdSequence, GET, PUT_APPEND,
};
use common::{KVError, ValidReply};
use labrpc::{Client, RequestMessage};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Once;

pub struct Clerk {
    init: Once,
    pub inner: ClerkInner,
}

impl Clerk {
    pub fn new(servers: Vec<Client>) -> Self {
        Self {
            init: Once::new(),
            inner: ClerkInner::new(servers),
        }
    }

    pub fn get<K: AsRef<str>>(&mut self, key: K) -> Option<String> {
        let (init, inner) = (&self.init, &mut self.inner);
        init.call_once(|| inner.commit_sentinel());
        let key = key.as_ref();
        loop {
            match inner.get(key.to_owned(), Default::default()) {
                Some(val) => return val,
                None => {}
            }
        }
    }

    pub fn put<K: AsRef<str>, V: AsRef<str>>(
        &mut self,
        key: K,
        value: V,
    ) -> Option<()> {
        let (init, inner) = (&self.init, &mut self.inner);
        init.call_once(|| inner.commit_sentinel());
        let key = key.as_ref();
        let value = value.as_ref();
        inner.put(key.to_owned(), value.to_owned(), Default::default())
    }

    pub fn append<K: AsRef<str>, V: AsRef<str>>(
        &mut self,
        key: K,
        value: V,
    ) -> Option<()> {
        let (init, inner) = (&self.init, &mut self.inner);
        init.call_once(|| inner.commit_sentinel());
        let key = key.as_ref();
        let value = value.as_ref();
        inner.append(key.to_owned(), value.to_owned(), Default::default())
    }
}

pub struct ClerkInner {
    servers: Vec<Client>,

    last_server_index: AtomicUsize,
    unique_id: UniqueIdSequence,

    executor: tokio::runtime::Runtime,
}

impl ClerkInner {
    pub fn new(servers: Vec<Client>) -> Self {
        Self {
            servers,

            last_server_index: AtomicUsize::new(0),
            unique_id: UniqueIdSequence::new(),

            executor: tokio::runtime::Builder::new_multi_thread()
                .thread_name("kvraft-clerk")
                .worker_threads(1)
                .build()
                .expect("Creating thread pool should not fail"),
        }
    }

    fn commit_sentinel(&mut self) {
        loop {
            let args = GetArgs {
                key: "".to_string(),
                unique_id: self.unique_id.zero(),
            };
            let reply: Option<GetReply> = self.call_rpc(GET, args, Some(1));
            if let Some(reply) = reply {
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
                    Err(KVError::Expired) | Err(KVError::Conflict) => {
                        // The client ID happens to be re-used. The request does
                        // not fail as "Duplicate", because another client has
                        // committed more than just the sentinel.
                        self.unique_id = UniqueIdSequence::new();
                    }
                    Err(_) => {}
                };
            };
        }
    }

    fn call_rpc<M, A, R>(
        &mut self,
        method: M,
        args: A,
        max_retry: Option<usize>,
    ) -> Option<R>
    where
        M: AsRef<str>,
        A: Serialize,
        R: DeserializeOwned + ValidReply,
    {
        let method = method.as_ref().to_owned();
        let data = RequestMessage::from(
            bincode::serialize(&args)
                .expect("Serialization of requests should not fail"),
        );

        let max_retry =
            std::cmp::max(max_retry.unwrap_or(usize::MAX), self.servers.len());

        let mut index = self.last_server_index.load(Ordering::Relaxed);
        for _ in 0..max_retry {
            let client = &self.servers[index];

            let reply = self
                .executor
                .block_on(client.call_rpc(method.clone(), data.clone()));
            if let Ok(reply) = reply {
                let ret: R = bincode::deserialize(reply.as_ref())
                    .expect("Deserialization of reply should not fail");
                if ret.is_reply_valid() {
                    self.last_server_index.store(index, Ordering::Relaxed);
                    return Some(ret);
                }
            }
            index += 1;
            index %= self.servers.len();
        }
        None
    }

    /// This function returns None when
    /// 1. No KVServer can be reached, or
    /// 2. No KVServer claimed to be the leader, or
    /// 3. When the KVServer committed the request but it was not passed
    /// back to the clerk. We must retry with a new unique_id.
    ///
    /// In all 3 cases the request can be retried.
    ///
    /// This function do not expect a Conflict request with the same unique_id.
    pub fn get(
        &mut self,
        key: String,
        options: KVRaftOptions,
    ) -> Option<Option<String>> {
        let args = GetArgs {
            key,
            unique_id: self.unique_id.inc(),
        };
        let reply: GetReply = self.call_rpc(GET, args, options.max_retry)?;
        match reply.result {
            Ok(val) => Some(val),
            Err(KVError::Conflict) => panic!("We should never see a conflict."),
            _ => None,
        }
    }

    fn put_append(
        &mut self,
        key: String,
        value: String,
        op: PutAppendEnum,
        options: KVRaftOptions,
    ) -> Option<()> {
        let args = PutAppendArgs {
            key,
            value,
            op,
            unique_id: self.unique_id.inc(),
        };
        let reply: PutAppendReply =
            self.call_rpc(PUT_APPEND, args, options.max_retry)?;
        assert!(reply.result.is_ok());
        Some(())
    }

    pub fn put(
        &mut self,
        key: String,
        value: String,
        options: KVRaftOptions,
    ) -> Option<()> {
        self.put_append(key, value, PutAppendEnum::Put, options)
    }

    pub fn append(
        &mut self,
        key: String,
        value: String,
        options: KVRaftOptions,
    ) -> Option<()> {
        self.put_append(key, value, PutAppendEnum::Append, options)
    }
}

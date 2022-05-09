use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Once;
use std::time::Duration;

use crate::common::{
    CommitSentinelArgs, CommitSentinelReply, GetArgs, GetReply, KVError,
    KVRaftOptions, PutAppendArgs, PutAppendEnum, PutAppendReply,
    UniqueIdSequence, ValidReply,
};
use crate::RemoteKvraft;

pub struct Clerk {
    init: Once,
    inner: ClerkInner,
}

impl Clerk {
    pub fn new(servers: Vec<impl RemoteKvraft>) -> Self {
        Self {
            init: Once::new(),
            inner: ClerkInner::new(servers),
        }
    }

    pub fn get<K: AsRef<str>>(&mut self, key: K) -> Option<String> {
        let inner = self.init_once();

        let key = key.as_ref();
        loop {
            if let Some(val) = inner.get(key.to_owned(), Default::default()) {
                return val;
            }
        }
    }

    pub fn put<K: AsRef<str>, V: AsRef<str>>(&mut self, key: K, value: V) {
        let inner = self.init_once();

        let key = key.as_ref();
        let value = value.as_ref();
        inner
            .put(key.to_owned(), value.to_owned(), Default::default())
            .expect("Put should never return error with unlimited retry.")
    }

    pub fn append<K: AsRef<str>, V: AsRef<str>>(&mut self, key: K, value: V) {
        let inner = self.init_once();

        let key = key.as_ref();
        let value = value.as_ref();
        inner
            .append(key.to_owned(), value.to_owned(), Default::default())
            .expect("Append should never return error with unlimited retry.")
    }

    pub fn init_once(&mut self) -> &mut ClerkInner {
        let (init, inner) = (&self.init, &mut self.inner);
        init.call_once(|| inner.commit_sentinel());
        &mut self.inner
    }
}

pub struct ClerkInner {
    servers: Vec<Box<dyn RemoteKvraft>>,

    last_server_index: AtomicUsize,
    unique_id: UniqueIdSequence,

    executor: tokio::runtime::Runtime,
}

impl ClerkInner {
    pub fn new(servers: Vec<impl RemoteKvraft>) -> Self {
        let servers = servers
            .into_iter()
            .map(|s| Box::new(s) as Box<dyn RemoteKvraft>)
            .collect();
        Self {
            servers,

            last_server_index: AtomicUsize::new(0),
            unique_id: UniqueIdSequence::new(),

            executor: tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("Creating thread pool should not fail"),
        }
    }

    fn commit_sentinel(&mut self) {
        loop {
            let args = CommitSentinelArgs {
                unique_id: self.unique_id.zero(),
            };
            let reply: Option<CommitSentinelReply> = self.retry_rpc(
                |remote, args| remote.commit_sentinel(args),
                args,
                Some(1),
            );
            if let Some(reply) = reply {
                match reply.result {
                    Ok(_) => {
                        // Discard the used unique_id.
                        self.unique_id.inc();
                        break;
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

    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);
    pub fn retry_rpc<'a, Func, Fut, Args, Reply>(
        &'a mut self,
        mut future_func: Func,
        args: Args,
        max_retry: Option<usize>,
    ) -> Option<Reply>
    where
        Args: Clone,
        Reply: ValidReply,
        Fut: Future<Output = std::io::Result<Reply>> + Send + 'a,
        Func: FnMut(&'a dyn RemoteKvraft, Args) -> Fut,
    {
        let max_retry =
            std::cmp::max(max_retry.unwrap_or(usize::MAX), self.servers.len());

        let mut index = self.last_server_index.load(Ordering::Relaxed);
        for _ in 0..max_retry {
            let client = &self.servers[index];
            let rpc_response = self.executor.block_on(async {
                tokio::time::timeout(
                    Self::DEFAULT_TIMEOUT,
                    future_func(client.as_ref(), args.clone()),
                )
                .await
            });
            let reply = match rpc_response {
                Ok(reply) => reply,
                Err(e) => Err(e.into()),
            };
            if let Ok(ret) = reply {
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
        let reply: GetReply = self.retry_rpc(
            |remote, args| remote.get(args),
            args,
            options.max_retry,
        )?;
        match reply.result {
            Ok(val) => Some(val),
            Err(KVError::Conflict) => panic!("We should never see a conflict."),
            _ => None,
        }
    }

    /// This function returns None when
    /// 1. No KVServer can be reached, or
    /// 2. No KVServer claimed to be the leader.
    ///
    /// Some(()) is returned if the request has been committed previously, under
    /// the assumption is that two different requests with the same unique_id
    /// must be identical.
    ///
    /// This function do not expect a Conflict request with the same unique_id.
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
        let reply: PutAppendReply = self.retry_rpc(
            |remote, args| remote.put_append(args),
            args,
            options.max_retry,
        )?;
        match reply.result {
            Ok(val) => Some(val),
            Err(KVError::Expired) => Some(()),
            Err(KVError::Conflict) => panic!("We should never see a conflict."),
            _ => None,
        }
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
